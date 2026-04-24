import os
from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware
from routers import collect, preprocess, public, analysis, auth, visualise

is_local = os.environ.get("ENV") == "local"

tags_metadata = [
    {
        "name": "Collect",
        "description": "Trigger data collection from the ABS API and store raw data in S3. Run these first before preprocessing.",
    },
    {
        "name": "Preprocess",
        "description": "Transform raw S3 data into ADAGE 3.0 schema and load into DynamoDB. Run the matching Collect endpoint first.",
    },
    {
        "name": "Public",
        "description": "Read-only endpoints serving processed economic data from DynamoDB. Data must be collected and preprocessed first.",
    },
    {
        "name": "Analysis",
        "description": "Compute trends and Pearson correlations across CPI, GDP and Unemployment datasets.",
    },
    {
        "name": "Auth",
        "description": "User registration, login and JWT-based authentication.",
    },
    {
        "name": "Visualise",
        "description": "Endpoints for visualising economic data.",
    },
]

app = FastAPI(
    title="Mango Financial Data API",
    description=(
        "A microservice providing Australian economic indicator data (CPI, GDP, Unemployment) "
        "sourced from the Australian Bureau of Statistics (ABS). "
        "All responses conform to the **ADAGE 3.0** schema.\n\n"
        "## Typical Workflow\n"
        "1. **Collect** — `POST /collect/{dataset}` to fetch from ABS and store in S3\n"
        "2. **Preprocess** — `POST /preprocess/{dataset}` then `POST /preprocess/clean{Dataset}` to load into DynamoDB\n"
        "3. **Query** — `GET /public/{dataset}` to retrieve data\n"
        "4. **Analyse** — `GET /public/analysis/*` for trends and correlations\n\n"
        "## ABS Dataset Coverage\n"
        "- **CPI**: Quarterly, configurable range\n"
        "- **GDP** (`ANA_IND_GVA`): Quarterly from **1960-Q1** to present\n"
        "- **Unemployment** (`LF`): Monthly data"
    ),
    version="2.0.0",
    docs_url=None,
    openapi_tags=tags_metadata,
    servers=[{"url": "https://x9rgu2z2vh.execute-api.us-east-1.amazonaws.com/prod", "description": "Production"}],
)

allowed_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "https://mango-frontend-zegj.vercel.app",

]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/docs", include_in_schema=False)
async def swagger_ui():
    openapi_url = "/openapi.json" if is_local else "/prod/openapi.json"
    return get_swagger_ui_html(openapi_url=openapi_url, title="Mango Financial Data API")

app.include_router(collect.router)
app.include_router(preprocess.router)
app.include_router(public.router)
app.include_router(analysis.router)
app.include_router(auth.router)
app.include_router(visualise.router)

handler = Mangum(app)
