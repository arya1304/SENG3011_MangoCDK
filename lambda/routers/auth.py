import os
from datetime import datetime, timezone, timedelta

import boto3
import hashlib
import jwt
from fastapi import APIRouter, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr

router = APIRouter(prefix="/auth", tags=["Auth"])

users_table = boto3.resource("dynamodb").Table(os.environ["USERS_TABLE_NAME"])  # type: ignore

JWT_SECRET = os.environ.get("JWT_SECRET", "default-secret")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24

security = HTTPBearer()


# request models
class RegisterRequest(BaseModel):
    email: EmailStr
    password: str
    name: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class UpdateRequest(BaseModel):
    name: str | None = None
    password: str | None = None


# helpers
def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def _create_token(email: str, name: str) -> str:
    payload = {
        "sub": email,
        "name": name,
        "iat": datetime.now(timezone.utc),
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    token = credentials.credentials
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"email": payload["sub"], "name": payload["name"]}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired.")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token.")


# endpoints
@router.post("/register")
def register(body: RegisterRequest):
    """
    POST /auth/register
    Register a new user with email, password, and name.
    """
    existing = users_table.get_item(Key={"email": body.email})
    if "Item" in existing:
        raise HTTPException(status_code=409, detail="Email already registered")

    users_table.put_item(Item={
        "email": body.email,
        "name": body.name,
        "password_hash": _hash_password(body.password),
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    token = _create_token(body.email, body.name)

    return {
        "message": "User registered successfully",
        "token": token,
        "user": {"email": body.email, "name": body.name},
    }


@router.post("/login")
def login(body: LoginRequest):
    """
    POST /auth/login
    Login with email and password, returns a JWT token.
    """
    result = users_table.get_item(Key={"email": body.email})
    user = result.get("Item")

    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if user["password_hash"] != _hash_password(body.password):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    token = _create_token(user["email"], user["name"])

    return {
        "message": "Login successful.",
        "token": token,
        "user": {"email": user["email"], "name": user["name"]},
    }


@router.put("/update")
def update_user(body: UpdateRequest, current_user: dict = Depends(get_current_user)):
    """
    PUT /auth/update
    Update the current user's name and/or password.
    Requires Authorization: Bearer <token> header.
    """
    if not body.name and not body.password:
        raise HTTPException(status_code=400, detail="Nothing to update. Provide name and/or password.")

    result = users_table.get_item(Key={"email": current_user["email"]})
    user = result.get("Item")
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    update_expr = []
    expr_values = {}

    if body.name:
        update_expr.append("#n = :name")
        expr_values[":name"] = body.name
    if body.password:
        update_expr.append("password_hash = :pw")
        expr_values[":pw"] = _hash_password(body.password)

    users_table.update_item(
        Key={"email": current_user["email"]},
        UpdateExpression="SET " + ", ".join(update_expr),
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames={"#n": "name"} if body.name else {},
    )

    new_name = body.name or current_user["name"]
    token = _create_token(current_user["email"], new_name)

    return {
        "message": "User updated successfully.",
        "token": token,
        "user": {"email": current_user["email"], "name": new_name},
    }


@router.get("/details")
def get_details(current_user: dict = Depends(get_current_user)):
    """
    GET /auth/details
    Returns the current authenticated user's details.
    Requires Authorization: Bearer <token> header.
    """
    return {"user": current_user}
