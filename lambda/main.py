from fastapi import FastAPI
from mangum import Mangum
from routers import collect, preprocess, public, analysis

app = FastAPI()

app.include_router(collect.router)
app.include_router(preprocess.router)
app.include_router(public.router)
app.include_router(analysis.router)

handler = Mangum(app)