import os
import uuid
import docker
import inspect
from typing import Optional, Type, List
from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, responses
from fastapi.params import Param
from starlette.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from multiprocessing import Process
from redis import Redis
from rq import Connection, Queue, Worker
from rq.job import Job
from irods.session import iRODSSession
import logging
import os.path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def collection():
    return os.environ["IRODS_COLLECTION"]

def open_session():
    host = os.environ["IRODS_HOST"]
    port = int(os.environ["IRODS_PORT"])
    user = os.environ["IRODS_USER"]
    password = os.environ["IRODS_PASSWORD"]
    zone = os.environ["IRODS_ZONE"]
    return iRODSSession(host=host, port=port, user=user, password=password, zone=zone)


def get_object(sess, irods_path, to_file_path):
    obj = sess.data_objects.get(irods_path)
    with obj.open() as f:
        content = f.read()
    with open(to_file_path, "wb") as outf:
        outf.write(content)


def put_object(sess, local_path=None, irods_path=None, content=None): 
    if local_path is not None:
        sess.data_objects.put(local_path, irods_path)
    else:
        with sess.data_objects.open(irods_path, 'w') as out_file:
            out_file.write(content)


def get(session, irods_path, local_path, recursive=False):
    """
        Download files from an iRODS server.
        Args:
            session (iRODS.session.iRODSSession): iRODS session
            irods_path (String): File or folder path to get
                from the iRODS server. Must be absolute path.
            local_path (String): local folder to place the downloaded files in
            recursive (Boolean): recursively get folders.
    """
    if session.data_objects.exists(irods_path):
        to_file_path = os.path.join(local_path, os.path.basename(irods_path))
        get_object(session, irods_path, to_file_path)
    elif session.collections.exists(irods_path):
        if recursive:
            coll = session.collections.get(irods_path)
            local_path = os.path.join(local_path, os.path.basename(irods_path))
            os.makedirs(local_path, exist_ok=True)

            for file_object in coll.data_objects:
                get(session, os.path.join(irods_path, file_object.path), local_path, True)
            for collection in coll.subcollections:
                get(session, collection.path, local_path, True)
        else:
            raise FileNotFoundError("Skipping directory " + irods_path)
    else:
        raise FileNotFoundError(irods_path + " Does not exist")

    
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
    Container: str = "hmasson/cellfie-standalone-app"
    ContainerTag: str = "latest"
    ArgumentsKeys: List[str] = [
        "SampleNumber",
        "Ref",
        "ThreshType",
        "PercentileOrValue",
        "Percentile",
        "Value",
        "LocalThresholdType",
        "PercentileLow",
        "PercentileHigh",
        "ValueLow",
        "ValueHigh"
    ]
    ArgumentsValues: List[str] = [
        "32",
        "MT_recon_2_2_entrez.mat",
        "local",
        "value",
        "25",
        "5",
        "minmaxmean",
        "25",
        "75",
        "5",
        "5"
    ]
    Command: str = "INPUT {SampleNumber} {Ref} {ThreshType} {PercentileOrValue} {Value} {LocalThresholdType} {ValueLow} {ValueHigh} OUTPUT"


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
    worker = Worker(q, connection=redis_connection)
    worker.work()

@app.post("/cellfie/run/upload_data")
async def run_with_uploaded_data(parameters: Parameters = Depends(Parameters.as_form), data: UploadFile = File(...)):
    #write data to memory
    irods_path = os.path.join(collection(), "data")
    task_id = str(uuid.uuid4())
    irods_path = os.path.join(irods_path, f"{task_id}-data")
    with open_session() as sess:
        sess.collections.create(irods_path, recurse=True)
        file_path = os.path.join(irods_path, "input.csv")
        content = await data.read()
        put_object(sess, irods_path=file_path, content=content)
    #instantiate task
    q.enqueue(run_cellfie_image, task_id=task_id, parameters=parameters, job_id=task_id, job_timeout=600)
    pWorker = Process(target = initWorker)
    pWorker.start()
    return {"task_id": task_id}

@app.get("/cellfie/status/{task_id}")
def get_task_status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=redis_connection)
        return {"task_status": job.get_status()}
    except:
        raise HTTPException(status_code=404, detail="Not found")

@app.get("/cellfie/results/{task_id}/{filename}")
def get_task_result(task_id: str, filename: str):
    local_path = os.path.join(collection(), "data")
    dir_path = os.path.join(local_path, f"{task_id}-data")
    file_path = os.path.join(dir_path, f"{filename}.csv")
    with open_session() as sess:
        if not sess.collections.exists(dir_path) or len(sess.collections.get(dir_path).data_objects) < 5:
            raise HTTPException(status_code=404, detail="Not found")    
        def iterfile():
            try:
                with sess.data_objects.open(file_path, mode="r") as file_data:
                    yield from file_data
            except Exception as e:
                logger.error(e)
                raise
        response = StreamingResponse(iterfile(), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=export.csv"
        return response

def run_cellfie_image(task_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')
    parameters_dict = dict(zip(parameters.ArgumentsKeys, parameters.ArgumentsValues))

    data_dir = "data"
    data_dir_task = f"{task_id}-data"
    container_data_dir = os.path.join("/app", data_dir)
    irods_data_dir_task = os.path.join(collection(), data_dir, data_dir_task)

    parsed_command = parameters.Command
    parsed_command.replace("INPUT", "/data/input.csv")
    parsed_command.replace("OUTPUT", "/data")

    #TODO: parse the arguments

    with open_session() as sess:
        get(sess, irods_data_dir_task, container_data_dir, recursive=True)
        client.containers.run(f"{parameters.Container}:{parameters.ContainerTag}",
                              volumes={
                                  os.path.join(local_path, data_dir, data_dir_task) : {'bind': '/data', 'mode': 'rw'},
                                  os.path.join(local_path, "CellFie/input") : {'bind': '/input', 'mode': 'rw'},
                              },
                              name=task_id,
                              working_dir="/input",
                              privileged=True,
                              remove=True,
                              command=parsed_command
        )
        output_dir = os.path.join(container_data_dir, data_dir_task)
        for filename in os.listdir(output_dir):
            put_object(sess, local_path=os.path.join(output_dir, filename), irods_path=os.path.join(irods_data_dir_task, filename))

def pull_imm_group(group_id: str, api_key: str):
    container = client.containers.run(
        "txscience/tx-immunespace-groups:0.2",
        remove=True,
        command=f"./ImmGeneBySampleMatrix.R -g {group_id} -a {api_key}"
    )
    return container.logs()

if __name__ == "__main__":
    print(pull_imm_group("group_id", "api_key"))