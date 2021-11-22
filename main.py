import datetime
import inspect
import os
import shutil
import uuid
from multiprocessing import Process
from typing import Type, Optional

import aiofiles
import docker
import pymongo
from bson.json_util import dumps, loads
from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Path
from fastapi import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from starlette.responses import StreamingResponse


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


app = FastAPI()

origins = [
    f"http://{os.getenv('HOSTNAME')}:8000",
    f"http://{os.getenv('HOSTNAME')}:80",
    f"http://{os.getenv('HOSTNAME')}",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = docker.from_env()

# queue
redis_connection = Redis(host='redis', port=6379, db=0)
q = Queue(connection=redis_connection, is_async=True, default_timeout=3600)

mongo_client = pymongo.MongoClient('mongodb://%s:%s@tx-persistence:27017/test' % (os.getenv('MONGO_NON_ROOT_USERNAME'), os.getenv('MONGO_NON_ROOT_PASSWORD')))
mongo_db = mongo_client["test"]
mongo_db_cellfie_submits_column = mongo_db["cellfie_submits"]
mongo_db_immunespace_downloads_column = mongo_db["immunespace_downloads"]
mongo_db_immunespace_cellfie_submits_column = mongo_db["immunespace_cellfie_submits"]


def initWorker():
    worker = Worker(q, connection=redis_connection)
    worker.work()


@app.post("/cellfie/submit")
async def cellfie_submit(email: str, parameters: Parameters = Depends(Parameters.as_form), expression_data: UploadFile = File(...),
                         phenotype_data: Optional[bytes] = File(None)):
    # write data to memory
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    task_id = str(uuid.uuid4())

    task_mapping_entry = {"task_id": task_id, "email": email, "status": None, "date_created": datetime.datetime.utcnow(), "start_date": None, "end_date": None}
    mongo_db_cellfie_submits_column.insert_one(task_mapping_entry)

    local_path = os.path.join(local_path, f"{task_id}-data")
    os.mkdir(local_path)

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path, 'w', encoding='utf-8') as f:
        f.write(parameters.json())
    f.close()

    file_path = os.path.join(local_path, expression_data.filename)
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await expression_data.read()
        await out_file.write(content)

    if phenotype_data is not None:
        phenotype_data_file_path = os.path.join(local_path, "phenoDataMatrix.csv")
        async with aiofiles.open(phenotype_data_file_path, 'wb') as out_file:
            await out_file.write(phenotype_data)

    # instantiate task
    q.enqueue(run_cellfie_image, task_id=task_id, parameters=parameters, job_id=task_id, job_timeout=3600, result_ttl=-1)
    p_worker = Process(target=initWorker)
    p_worker.start()
    return {"task_id": task_id}


def run_cellfie_image(task_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(task_id, connection=redis_connection)
    task_mapping_entry = {"task_id": task_id}
    new_values = {"$set": {"start_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_cellfie_submits_column.update_one(task_mapping_entry, new_values)

    global_value = parameters.Percentile if parameters.PercentileOrValue == "percentile" else parameters.Value
    local_values = f"{parameters.PercentileLow} {parameters.PercentileHigh}" if parameters.PercentileOrValue == "percentile" else f"{parameters.ValueLow} {parameters.ValueHigh}"

    image = "hmasson/cellfie-standalone-app:v2"
    volumes = {
        os.path.join(local_path, f"data/{task_id}-data"): {'bind': '/data', 'mode': 'rw'},
        os.path.join(local_path, "CellFie/input"): {'bind': '/input', 'mode': 'rw'},
    }
    command = f"/data/geneBySampleMatrix.csv {parameters.SampleNumber} {parameters.Ref} {parameters.ThreshType} {parameters.PercentileOrValue} {global_value} {parameters.LocalThresholdType} {local_values} /data"
    client.containers.run(image, volumes=volumes, name=task_id, working_dir="/input", privileged=True, remove=True, command=command)

    new_values = {"$set": {"end_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_cellfie_submits_column.update_one(task_mapping_entry, new_values)


@app.delete("/cellfie/delete/{task_id}")
async def cellfie_delete(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    task_query = {"task_id": task_id}
    mongo_db_cellfie_submits_column.delete_one(task_query)

    local_path = os.path.join(local_path, f"{task_id}-data")
    shutil.rmtree(local_path)

    try:
        job = Job.fetch(task_id, connection=redis_connection)
        job.delete(remove_from_queue=True)
    except:
        raise HTTPException(status_code=404, detail="Not found")

    return {"status": "done"}


@app.get("/cellfie/parameters/{task_id}")
async def cellfie_parameters(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    local_path = os.path.join(local_path, f"{task_id}-data")

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path) as f:
        param_path_contents = eval(f.read())
    f.close()

    parameter_object = Parameters(**param_path_contents)
    return parameter_object.dict()


@app.get("/cellfie/task_ids/{email}")
async def cellfie_ids(email: str):
    query = {"email": email}
    ret = list(map(lambda a: a, mongo_db_cellfie_submits_column.find(query, {"_id": 0, "task_id": 1})))
    return ret


@app.get("/cellfie/status/{task_id}")
def cellfie_status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=redis_connection)
        ret = {"status": job.get_status()}
        task_mapping_entry = {"task_id": task_id}
        new_values = {"$set": ret}
        mongo_db_cellfie_submits_column.update_one(task_mapping_entry, new_values)
        return ret
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/cellfie/metadata/{task_id}")
def cellfie_metadata(task_id: str):
    try:
        task_mapping_entry = {"task_id": task_id}
        projection = {"_id": 0, "task_id": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
        entry = mongo_db_cellfie_submits_column.find(task_mapping_entry, projection)
        return loads(dumps(entry.next()))
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/cellfie/results/{task_id}/{filename}")
def cellfie_results(task_id: str, filename: str = Path(...,
                                                       description="Valid file name values include: detailScoring, geneBySampleMatrix, phenoDataMatrix, score, score_binary, & taskInfo")):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    dir_path = os.path.join(local_path, f"{task_id}-data")
    file_path = os.path.join(dir_path, f"{filename}.csv")
    if not os.path.isdir(dir_path) or not os.path.exists(file_path):
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


@app.post("/immunespace/download")
async def immunespace_download(email: str, group: str, apikey: str):
    # write data to memory
    immunespace_download_query = {"email": email, "group_id": group, "apikey": apikey}
    projection = {"_id": 0, "immunespace_download_id": 1, "email": 1, "group_id": 1, "apikey": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
    entry = mongo_db_immunespace_downloads_column.find(immunespace_download_query, projection)

    if entry.count() > 0:
        immunespace_download_id = entry.next()["immunespace_download_id"]

        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
        if os.path.exists(local_path) and len(os.listdir(local_path)) == 0:
            q.enqueue(run_immunespace_download, immunespace_download_id=immunespace_download_id, group=group, apikey=apikey, job_id=immunespace_download_id, job_timeout=3600,
                      result_ttl=-1)
            p_worker = Process(target=initWorker)
            p_worker.start()

        return {"immunespace_download_id": immunespace_download_id}
    else:
        immunespace_download_id = str(uuid.uuid4())[:8]

        task_mapping_entry = {"immunespace_download_id": immunespace_download_id, "email": email, "group_id": group, "apikey": apikey, "status": None,
                              "date_created": datetime.datetime.utcnow(), "start_date": None, "end_date": None}
        mongo_db_immunespace_downloads_column.insert_one(task_mapping_entry)

        local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        local_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
        os.mkdir(local_path)

        # instantiate task
        q.enqueue(run_immunespace_download, immunespace_download_id=immunespace_download_id, group=group, apikey=apikey, job_id=immunespace_download_id, job_timeout=3600,
                  result_ttl=-1)
        p_worker = Process(target=initWorker)
        p_worker.start()
        return {"immunespace_download_id": immunespace_download_id}


@app.get("/immunespace/download/ids/{email}")
async def immunespace_download_ids(email: str):
    query = {"email": email}
    ret = list(map(lambda a: a, mongo_db_immunespace_downloads_column.find(query, {"_id": 0, "immunespace_download_id": 1})))
    return ret


@app.get("/immunespace/download/metadata/{immunespace_download_id}")
def immunespace_download_metadata(immunespace_download_id: str):
    try:
        task_mapping_entry = {"immunespace_download_id": immunespace_download_id}
        projection = {"_id": 0, "immunespace_download_id": 1, "email": 1, "group_id": 1, "apikey": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
        entry = mongo_db_immunespace_downloads_column.find(task_mapping_entry, projection)
        return loads(dumps(entry.next()))
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/immunespace/download/status/{immunespace_download_id}")
def immunespace_download_status(immunespace_download_id: str):
    try:
        job = Job.fetch(immunespace_download_id, connection=redis_connection)
        ret = {"status": job.get_status()}
        task_mapping_entry = {"immunespace_download_id": immunespace_download_id}
        new_values = {"$set": ret}
        mongo_db_cellfie_submits_column.update_one(task_mapping_entry, new_values)
        return ret
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/immunespace/download/results/{immunespace_download_id}/{filename}")
def immunespace_download_results(immunespace_download_id: str, filename: str = Path(...,
                                                                                    description="Valid file name values include: geneBySampleMatrix & phenoDataMatrix")):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    dir_path = os.path.join(local_path, f"{immunespace_download_id}-immunespace-data")
    file_path = os.path.join(dir_path, f"{filename}.csv")
    if not os.path.isdir(dir_path) or not os.path.exists(file_path):
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


def run_immunespace_download(immunespace_download_id: str, group: str, apikey: str):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(immunespace_download_id, connection=redis_connection)
    task_mapping_entry = {"immunespace_download_id": immunespace_download_id}
    new_values = {"$set": {"start_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_immunespace_downloads_column.update_one(task_mapping_entry, new_values)

    image = "txscience/tx-immunespace-groups:0.3"
    volumes = {os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/data', 'mode': 'rw'}}
    command = f"-g \"{group}\" -a \"{apikey}\" -o /data"
    client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-groups", working_dir="/data", privileged=True, remove=True, command=command)
    logger.logger.warn(msg=f"{datetime.datetime.utcnow()} - finished txscience/tx-immunespace-groups:0.3")

    image = "txscience/fuse-mapper-immunespace:0.1"
    volumes = {os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/data', 'mode': 'rw'}}
    command = f"-g /data/geneBySampleMatrix.csv -p /data/phenoDataMatrix.csv"
    client.containers.run(image, volumes=volumes, name=f"{immunespace_download_id}-immunespace-mapper", working_dir="/data", privileged=True, remove=True, command=command)
    logger.logger.warn(msg=f"{datetime.datetime.utcnow()} - finished fuse-mapper-immunespace:0.1")

    new_values = {"$set": {"end_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_immunespace_downloads_column.update_one(task_mapping_entry, new_values)


@app.post("/immunespace/cellfie/submit")
async def immunespace_cellfie_submit(immunespace_download_id: str, parameters: Parameters = Depends(Parameters.as_form)):
    # write data to memory
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    task_id = str(uuid.uuid4())

    local_path = os.path.join(local_path, f"{task_id}-data")
    os.mkdir(local_path)

    task_mapping_entry = {"task_id": task_id, "immunespace_download_id": immunespace_download_id, "status": None, "date_created": datetime.datetime.utcnow(), "start_date": None,
                          "end_date": None}
    mongo_db_immunespace_cellfie_submits_column.insert_one(task_mapping_entry)

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path, 'w', encoding='utf-8') as f:
        f.write(parameters.json())
    f.close()

    # instantiate task
    q.enqueue(run_immunespace_cellfie_image, task_id=task_id, immunespace_download_id=immunespace_download_id, parameters=parameters, job_id=task_id, job_timeout=3600,
              result_ttl=-1)
    p_worker = Process(target=initWorker)
    p_worker.start()
    return {"task_id": task_id}


def run_immunespace_cellfie_image(task_id: str, immunespace_download_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(task_id, connection=redis_connection)
    task_mapping_entry = {"task_id": task_id}
    new_values = {"$set": {"start_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_immunespace_cellfie_submits_column.update_one(task_mapping_entry, new_values)

    global_value = parameters.Percentile if parameters.PercentileOrValue == "percentile" else parameters.Value
    local_values = f"{parameters.PercentileLow} {parameters.PercentileHigh}" if parameters.PercentileOrValue == "percentile" else f"{parameters.ValueLow} {parameters.ValueHigh}"

    image = "hmasson/cellfie-standalone-app:v2"
    volumes = {
        os.path.join(local_path, f"data/{immunespace_download_id}-immunespace-data"): {'bind': '/immunespace-data', 'mode': 'rw'},
        os.path.join(local_path, f"data/{task_id}-data"): {'bind': '/data', 'mode': 'rw'},
        os.path.join(local_path, "CellFie/input"): {'bind': '/input', 'mode': 'rw'},
    }
    command = f"/immunespace-data/geneBySampleMatrix.csv {parameters.SampleNumber} {parameters.Ref} {parameters.ThreshType} {parameters.PercentileOrValue} {global_value} {parameters.LocalThresholdType} {local_values} /data"
    client.containers.run(image, volumes=volumes, name=task_id, working_dir="/input", privileged=True, remove=True, command=command)

    new_values = {"$set": {"end_date": datetime.datetime.utcnow(), "status": job.get_status()}}
    mongo_db_immunespace_cellfie_submits_column.update_one(task_mapping_entry, new_values)


@app.get("/immunespace/cellfie/status/{task_id}")
def immunespace_cellfie_status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=redis_connection)
        ret = {"status": job.get_status()}
        task_mapping_entry = {"task_id": task_id}
        new_values = {"$set": ret}
        mongo_db_immunespace_cellfie_submits_column.update_one(task_mapping_entry, new_values)
        return ret
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/immunespace/cellfie/results/{task_id}/{filename}")
def immunespace_results(task_id: str, filename: str = Path(...,
                                                           description="Valid file name values include: detailScoring, score, score_binary, & taskInfo")):
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


@app.get("/immunespace/cellfie/task_ids/{email}")
async def cellfie_ids(email: str):
    task_mapping_entry = {"email": email}
    projection = {"_id": 0, "immunespace_download_id": 1}
    immunespace_download_identifiers = list(map(lambda a: a["immunespace_download_id"], mongo_db_immunespace_downloads_column.find(task_mapping_entry, projection)))
    logger.logger.warn(msg=f"{immunespace_download_identifiers}")
    immunespace_download_query = {"immunespace_download_id": {"$in": immunespace_download_identifiers}}
    ret = list(map(lambda a: a, mongo_db_immunespace_cellfie_submits_column.find(immunespace_download_query, {"_id": 0, "task_id": 1})))
    return ret


@app.get("/immunespace/cellfie/metadata/{task_id}")
def cellfie_metadata(task_id: str):
    try:
        task_mapping_entry = {"task_id": task_id}
        projection = {"_id": 0, "task_id": 1, "immunespace_download_id": 1, "status": 1, "date_created": 1, "start_date": 1, "end_date": 1}
        entry = mongo_db_immunespace_cellfie_submits_column.find(task_mapping_entry, projection)
        return loads(dumps(entry.next()))
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.delete("/immunespace/cellfie/delete/{task_id}")
async def cellfie_delete(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    task_query = {"task_id": task_id}
    mongo_db_immunespace_cellfie_submits_column.delete_one(task_query)

    local_path = os.path.join(local_path, f"{task_id}-data")
    shutil.rmtree(local_path)

    try:
        job = Job.fetch(task_id, connection=redis_connection)
        job.delete(remove_from_queue=True)
    except:
        raise HTTPException(status_code=404, detail="Not found")

    return {"status": "done"}


@app.get("/immunespace/cellfie/parameters/{task_id}")
async def immunespace_cellfie_parameters(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    local_path = os.path.join(local_path, f"{task_id}-data")

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path) as f:
        param_path_contents = eval(f.read())
    f.close()

    parameter_object = Parameters(**param_path_contents)
    return parameter_object.dict()
