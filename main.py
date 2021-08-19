import json
import os
import aiofiles
import uuid
import docker
import time
import inspect
from typing import Optional, Type
from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, responses
from fastapi.params import Param
from starlette.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from multiprocessing import Process
from redis import Redis
from rq import Connection, Queue, Worker

def as_form(cls: Type[BaseModel]):
    """
    Adds an as_form class method to decorated models. The as_form class method
    can be used with FastAPI endpoints
    """
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

def initWorker():
    worker = Worker(Queue(connection=redis_connection), connection=redis_connection)
    worker.work()

@app.post("/cellfie/run/upload_data")
async def run_with_uploaded_data(parameters: Parameters = Depends(Parameters.as_form), data: UploadFile = File(...)):
    #write data to memory
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tmp")
    task_id = str(uuid.uuid4())
    filename = f"{task_id}-input.csv"
    file_path = os.path.join(local_path, filename)
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await data.read()
        await out_file.write(content)
    #instantiate task
    q.enqueue(run_cellfie_image, task_id=task_id, parameters=parameters, job_id=task_id, job_timeout=600)
    pWorker = Process(target = initWorker)
    pWorker.start()
    return {"task_id": task_id}

@app.get("/cellfie/results/{task_id}/{name}")
def get_task_result(task_id: str, name: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".tmp")
    dir_path = os.path.join(local_path, f"{task_id}-output")
    file_path = os.path.join(dir_path, f"{name}.csv")
    if not os.path.isdir(dir_path) or len(os.listdir(dir_path)) == 0:
        raise HTTPException(status_code=404, detail="Not found")
    def iterfile():  
        with open(file_path, mode="rb") as file_data:
            yield from file_data  
    response = StreamingResponse(iterfile(), media_type="text/csv")
    response.headers["Content-Disposition"] = "attachment; filename=export.csv"
    return response

def run_cellfie_image(task_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')
    filename = f"{task_id}-input.csv"

    global_value = parameters.Percentile if parameters.PercentileOrValue == "percentile" else parameters.Value
    local_values = f"{parameters.PercentileLow} {parameters.PercentileHigh}" if parameters.PercentileOrValue == "percentile" else f"{parameters.ValueLow} {parameters.ValueHigh}"

    client.containers.run("hmasson/cellfie-standalone-app:latest",
        volumes={
            os.path.join(local_path, f".tmp/{filename}"): {'bind': '/HPA.csv', 'mode': 'rw'},
            os.path.join(local_path, "CellFie/input"): {'bind': '/input', 'mode': 'rw'},
            os.path.join(local_path, f".tmp/{task_id}-output"): {'bind': '/outtmp', 'mode': 'rw'}
        },
        detach=True,
        working_dir="/input",
        privileged=True,
        remove=True,
        command=f"../HPA.csv {parameters.SampleNumber} {parameters.Ref} {parameters.ThreshType} {parameters.PercentileOrValue} {global_value} {parameters.LocalThresholdType} {local_values} ../outtmp"
    )
    return task_id