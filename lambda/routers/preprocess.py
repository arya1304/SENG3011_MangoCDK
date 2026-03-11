from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/preprocess")

@router.post("/cpi")
def preprocess_cpi():
    """
    POST /preprocess/cpi to preprocess CPI data and return
    """
    return {"message": "Preprocessing completed"}

@router.post("/gdp")
def preprocess_gdp():
    """
    POST /preprocess/gdp to preprocess GDP data and return
    """
    return {"message": "Preprocessing completed"}

@router.post("/unemployment")
def preprocess_unemployment():
    """
    POST /preprocess/unemployment to preprocess unemployment data and return
    """
    return {"message": "Preprocessing completed"}

@router.post("/clean")
def preprocess_clean():
    """
    POST /preprocess/clean to clean the data and return
    """
    return {"message": "Data cleaning completed"}