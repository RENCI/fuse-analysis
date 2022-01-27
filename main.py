import datetime
import decimal
import inspect
import json
import os
import shutil
import uuid
from datetime import datetime
from multiprocessing import Process
from typing import Type, Optional

import aiofiles
import docker
from bson.json_util import dumps, loads
from docker.errors import ContainerError
from fastapi import FastAPI, File, UploadFile, Form, Depends, HTTPException, Path
from fastapi.logger import logger
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
from starlette.responses import StreamingResponse
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, Integer, Date, ForeignKey, DateTime
from sqlalchemy.orm import relationship, backref

engine = create_engine(os.getenv('DATABASE_URL'), isolation_level="SERIALIZABLE")
Session = sessionmaker(bind=engine)

Base = declarative_base()


class CellfieUser(Base):
    __tablename__ = 'cellfie_users'

    id = Column(Integer, primary_key=True)
    email = Column(String)

    def __init__(self, email):
        self.email = email

    def to_json(self):
        return dict(id=self.id, email=self.email)

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result

class CellfieTask(Base):
    __tablename__ = 'cellfie_tasks'

    id = Column(Integer, primary_key=True)
    title = Column(String)
    task_id = Column(String)
    status = Column(String)
    stderr = Column(String)
    date_created = Column(DateTime)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    user_fid = Column(Integer, ForeignKey('cellfie_users.id'))
    cellfie_user = relationship("CellfieUser", backref=backref("cellfie_tasks", uselist=False))

    def __init__(self, task_id, status, date_created, cellfie_user):
        self.task_id = task_id
        self.status = status
        self.date_created = date_created
        self.cellfie_user = cellfie_user

    def to_json(self):
        return dict(id=self.id, email=self.email, date_created=self.date_created)

    def to_dict(self):
        result = {}
        for key in self.__mapper__.c.keys():
            if getattr(self, key) is not None:
                result[key] = str(getattr(self, key))
            else:
                result[key] = getattr(self, key)
        return result


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


Base.metadata.create_all(engine)
session = Session()

app = FastAPI()

origins = [
    f"http://{os.getenv('HOSTNAME')}:8000",
    f"http://{os.getenv('HOSTNAME')}:80",
    f"http://{os.getenv('HOSTNAME')}",
    "http://localhost:8000",
    "http://localhost:80",
    "http://localhost",
    "*",
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

redis_connection = Redis(host=os.getenv('REDIS_HOST'), port=os.getenv('REDIS_PORT'), db=0)
q = Queue(connection=redis_connection, is_async=True, default_timeout=3600)


def initWorker():
    worker = Worker(q, connection=redis_connection)
    worker.work()


@app.post("/submit")
async def submit(email: str, parameters: Parameters = Depends(Parameters.as_form), expression_data: UploadFile = File(...),
                 phenotype_data: Optional[bytes] = File(None)):
    # write data to memory
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    task_id = str(uuid.uuid4())

    found_cellfie_user: CellfieUser = session.query(CellfieUser).filter(CellfieUser.email == email).first()
    if found_cellfie_user is None:
        session.add(CellfieUser(email))
        session.commit()
        found_cellfie_user = session.query(CellfieUser).filter(CellfieUser.email == email).first()

    session.add(CellfieTask(task_id, "pending", datetime.utcnow(), found_cellfie_user))
    session.commit()

    local_path = os.path.join(local_path, f"{task_id}-data")
    os.mkdir(local_path)

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path, 'w', encoding='utf-8') as f:
        f.write(parameters.json())
    f.close()

    file_path = os.path.join(local_path, "geneBySampleMatrix.csv")
    async with aiofiles.open(file_path, 'wb') as out_file:
        content = await expression_data.read()
        await out_file.write(content)

    if phenotype_data is not None:
        phenotype_data_file_path = os.path.join(local_path, "phenoDataMatrix.csv")
        async with aiofiles.open(phenotype_data_file_path, 'wb') as out_file:
            await out_file.write(phenotype_data)

    # instantiate task
    q.enqueue(run_image, task_id=task_id, parameters=parameters, job_id=task_id, job_timeout=3600, result_ttl=-1)
    p_worker = Process(target=initWorker)
    p_worker.start()
    return {"task_id": task_id}


def run_image(task_id: str, parameters: Parameters):
    local_path = os.getenv('HOST_ABSOLUTE_PATH')

    job = Job.fetch(task_id, connection=redis_connection)

    found_cellfie_task: CellfieTask = session.query(CellfieTask).filter(CellfieTask.task_id == task_id).first()
    if found_cellfie_task is None:
        logger.warn(msg="should never get here")

    found_cellfie_task.start_date = datetime.utcnow()
    found_cellfie_task.status = job.get_status()
    logger.warn(msg=f"{datetime.utcnow()} - {json.dump(found_cellfie_task.to_dict())}")
    session.commit()

    global_value = parameters.Percentile if parameters.PercentileOrValue == "percentile" else parameters.Value
    local_values = f"{parameters.PercentileLow} {parameters.PercentileHigh}" if parameters.PercentileOrValue == "percentile" else f"{parameters.ValueLow} {parameters.ValueHigh}"

    image = "hmasson/cellfie-standalone-app:v2"
    volumes = {
        os.path.join(local_path, f"data/{task_id}-data"): {'bind': '/data', 'mode': 'rw'},
        os.path.join(local_path, "CellFie/input"): {'bind': '/input', 'mode': 'rw'},
    }
    command = f"/data/geneBySampleMatrix.csv {parameters.SampleNumber} {parameters.Ref} {parameters.ThreshType} {parameters.PercentileOrValue} {global_value} {parameters.LocalThresholdType} {local_values} /data"
    try:
        client.containers.run(image, volumes=volumes, name=task_id, working_dir="/input", privileged=True, remove=True, command=command)
    except ContainerError as err:
        found_cellfie_task.end_date = datetime.utcnow()
        found_cellfie_task.status = "failed"
        found_cellfie_task.stderr = err.stderr.decode('utf-8')
        session.commit()
        return

    found_cellfie_task.end_date = datetime.utcnow()
    found_cellfie_task.status = job.get_status()
    session.commit()


@app.delete("/delete/{task_id}")
async def delete(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    session.delete().where(CellfieTask.task_id == task_id)
    session.commit()

    local_path = os.path.join(local_path, f"{task_id}-data")
    shutil.rmtree(local_path)

    try:
        job = Job.fetch(task_id, connection=redis_connection)
        job.delete(remove_from_queue=True)
    except:
        raise HTTPException(status_code=404, detail="Not found")

    return {"status": "done"}


@app.get("/parameters/{task_id}")
async def parameters(task_id: str):
    local_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    local_path = os.path.join(local_path, f"{task_id}-data")

    param_path = os.path.join(local_path, "parameters.json")
    with open(param_path) as f:
        param_path_contents = eval(f.read())
    f.close()

    parameter_object = Parameters(**param_path_contents)
    return parameter_object.dict()


def alchemyencoder(obj):
    """JSON encoder function for SQLAlchemy special classes."""
    if isinstance(obj, datetime.date):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)


@app.get("/task_ids/{email}")
async def task_ids(email: str):
    cellfie_user: CellfieUser = session.query(CellfieUser).filter(CellfieUser.email == email).first()
    if cellfie_user is None:
        raise HTTPException(status_code=404, detail="Not found")
    logger.warn(msg=f"{datetime.utcnow()} - {cellfie_user.to_json()}")
    cellfie_tasks = session.query(CellfieTask).filter(CellfieTask.cellfie_user == cellfie_user).all()
    return loads(dumps([dict(r.to_dict()) for r in cellfie_tasks], default=alchemyencoder))


@app.get("/status/{task_id}")
def status(task_id: str):
    try:
        job = Job.fetch(task_id, connection=redis_connection)
        ret = {"status": job.get_status()}

        found_cellfie_task: CellfieTask = session.query(CellfieTask).filter(CellfieTask.task_id == task_id).first()
        if found_cellfie_task is None:
            logger.warn(msg="should never get here")

        found_cellfie_task.status = job.get_status()
        session.commit()
        return ret
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/metadata/{task_id}")
def metadata(task_id: str):
    try:
        found_cellfie_task: CellfieTask = session.query(CellfieTask).filter(CellfieTask.task_id == task_id).first()
        if found_cellfie_task is None:
            logger.warn(msg="should never get here")
        return loads(dumps(found_cellfie_task))
    except:
        raise HTTPException(status_code=404, detail="Not found")


@app.get("/results/{task_id}/{filename}")
def results(task_id: str,
            filename: str = Path(..., description="Valid file name values include: detailScoring, geneBySampleMatrix, phenoDataMatrix, score, score_binary, & taskInfo")):
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
