import os
import aiofiles
import uuid
import docker
import inspect
import pymongo
import json
from typing import Optional, Type
from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, responses
from fastapi.params import Param
import redis
from starlette.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from multiprocessing import Process
from redis import Redis
from rq import Connection, Queue, Worker
from rq.job import Job


def as_form(cls: Type[BaseModel]):
    new_params = [
        inspect.Parameter(
            field.alias,
            inspect.Parameter.POSITIONAL_ONLY,
            default=(Form(field.default) if not field.required else Form(...)),
        )
        for field in cls.__fields__.values()
    ]

    async def _as_form(**data):
        return cls(**data)

    sig = inspect.signature(_as_form)
    sig = sig.replace(parameters=new_params)
    _as_form.__signature__ = sig
    setattr(cls, "as_form", _as_form)
    return cls


@as_form
class Parameters(BaseModel):
    SampleNumber: int = 32
    Ref: str = "MT_recon_2_2_entrez.mat"
    ThreshType: str = "local"
    PercentileOrValue: str = "value"
    Percentile: int = 25
    Value: int = 5
    LocalThresholdType: str = "minmaxmean"
    PercentileLow: int = 25
    PercentileHigh: int = 75
    ValueLow: int = 5
    ValueHigh: int = 5

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = docker.from_env()

# queue
redis_connection = Redis(host='redis', port=6379, db=0)
q = Queue(connection=redis_connection, is_async=True)

mongo_client = pymongo.MongoClient('mongodb://%s:%s@tx-persistence:27017/test' % (os.getenv('MONGO_NON_ROOT_USERNAME'), os.getenv('MONGO_NON_ROOT_PASSWORD')))
mongo_db = mongo_client["test"]
mongo_db_email_task_mapping_column = mongo_db["email_task_mapping"]


def initWorker():
    worker = Worker(q, connection=redis_connection)
    worker.work()


@app.post("/cellfie/run/upload_data")
async def run_with_uploaded_data(email: str, parameters: Parameters = Depends(Parameters.as_form),
                                 data: UploadFile = File(...)):
    # write data to memory
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    task_id = str(uuid.uuid4())

    email_task_mapping_entry = {"email": email, "task_id": task_id}
    mongo_db_email_task_mapping_column.insert_one(email_task_mapping_entry)

    local_path = os.path.join(local_path, f"{task_id}-data")
    os.mkdir(local_path)

    param_path = os.path.join(local_path, "parameters.json")
    param_data = parameters.toJSON().replace('\n', '')
    with open(param_path, 'w', encoding='utf-8') as f:
        json.dump(param_data, f, ensure_ascii=False, indent=4)

    file_path = os.path.join(local_path, "input.csv")
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await data.read()
        await out_file.write(content)

    # instantiate task
    q.enqueue(run_cellfie_image, task_id=task_id, parameters=parameters, job_id=task_id, job_timeout=600)
    pWorker = Process(target=initWorker)
    pWorker.start()
    return {"task_id": task_id}

@app.get("/cellfie/get_run_parameters/{task_id}")
async def get_run_parameters(task_id: str):

    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    local_path = os.path.join(local_path, f"{task_id}-data")

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path) as f:
        loaded_json = json.load(f)

    return loaded_json

@app.get("/cellfie/get_task_ids/{email}")
async def get_task_ids(email: str):
    query = {"email": email}
    ret = list(map(lambda a: a, mongo_db_email_task_mapping_column.find(query, {"_id": 0, "task_id": 1})))
    return ret

@app.get("/cellfie/status/{task_id}")
def get_task_status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=redis_connection)
        return {"task_status": job.get_status()}
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/cellfie/results/{task_id}/{filename}")
def get_task_result(task_id: str, filename: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    dir_path = os.path.join(local_path, f"{task_id}-data")
    file_path = os.path.join(dir_path, f"{filename}.csv")
    if not os.path.isdir(dir_path) or len(os.listdir(dir_path)) < 5:
        raise HTTPException(status_code=404, detail="Not found")

    def iterfile():
        try:
            with open(file_path, mode="rb") as file_data:
                yield from file_data
        except:
            raise Exception()

    response = StreamingResponse(iterfile(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response


def run_cellfie_image(task_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    global_value = parameters.Percentile if parameters.PercentileOrValue == "percentile" else parameters.Value
    local_values = f"{parameters.PercentileLow} {parameters.PercentileHigh}" if parameters.PercentileOrValue == "percentile" else f"{parameters.ValueLow} {parameters.ValueHigh}"

    client.containers.run("hmasson/cellfie-standalone-app:v2",
                          volumes={
                              os.path.join(local_path, f"data/{task_id}-data"): {'bind': '/data', 'mode': 'rw'},
                              os.path.join(local_path, "CellFie/input"): {'bind': '/input', 'mode': 'rw'},
                          },
                          name=task_id,
                          working_dir="/input",
                          privileged=True,
                          remove=True,
                          command=f"/data/input.csv {parameters.SampleNumber} {parameters.Ref} {parameters.ThreshType} {parameters.PercentileOrValue} {global_value} {parameters.LocalThresholdType} {local_values} /data"
                          )
