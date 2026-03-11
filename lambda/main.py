from fastapi import FastAPI
from mangum import Mangum
from routers import collect, preprocess, public

app = FastAPI()

app.include_router(collect.router)
app.include_router(preprocess.router)
app.include_router(public.router)

handler = Mangum(app)