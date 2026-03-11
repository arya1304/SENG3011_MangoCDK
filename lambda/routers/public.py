from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/public")

@router.get("/cpi")
def get_cpi():
    """
    GET /public/cpi?start=2023-Q1&end=2024-Q4
    Retrieve CPI data from the database
    """
    return {"message": "CPI data retrieved successfully"}

@router.get("/unemployment")
def get_unemployment():
    """
    GET /public/unemployment?start=2023-Q1&end=2024-Q4
    Retrieve unemployment data from the database
    """
    return {"message": "Unemployment data retrieved successfully"}

@router.get("/gdp")
def get_gdp():
    """
    GET /public/gdp?start=2023-Q1&end=2024-Q4
    Retrieve GDP data from the database
    """
    return {"message": "GDP data retrieved successfully"}