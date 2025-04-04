from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
from config import settings
import models
from database import get_db
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

def verify_password(plain_password, hashed_password):
    logger.info(f"Verifying password for hash: {hashed_password[:20]}...")
    result = pwd_context.verify(plain_password, hashed_password)
    logger.info(f"Password verification result: {result}")
    return result

def get_password_hash(password):
    logger.info("Generating password hash...")
    hashed = pwd_context.hash(password)
    logger.info(f"Generated hash: {hashed[:20]}...")
    return hashed

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    try:
        logger.info(f"Creating access token with data: {data}")
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        logger.info(f"Token data to encode: {to_encode}")
        logger.info(f"Using secret key: {settings.SECRET_KEY[:10]}...")
        encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
        logger.info("Token created successfully")
        return encoded_jwt
    except Exception as e:
        logger.error(f"Error creating access token: {str(e)}")
        raise

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        logger.info("Attempting to get current user")
        logger.info(f"Token received: {token[:20]}...")
        
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
        try:
            logger.info("Decoding JWT token")
            payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
            logger.info(f"Token payload: {payload}")
            
            username: str = payload.get("sub")
            if username is None:
                logger.error("Username not found in token")
                raise credentials_exception
                
            logger.info(f"Looking up user: {username}")
            user = db.query(models.User).filter(models.User.username == username).first()
            
            if user is None:
                logger.error(f"User not found in database: {username}")
                raise credentials_exception
                
            logger.info(f"User found: {user.username} (ID: {user.id})")
            return user
            
        except JWTError as jwt_error:
            logger.error(f"JWT Error: {str(jwt_error)}")
            raise credentials_exception
            
    except Exception as e:
        logger.error(f"Unexpected error in get_current_user: {str(e)}")
        logger.exception("Full error traceback:")
        raise HTTPException(
            status_code=500,
            detail=f"Authentication error: {str(e)}"
        ) 