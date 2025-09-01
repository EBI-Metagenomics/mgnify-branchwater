from functools import wraps
from typing import Any, Dict, List, Optional, Literal
from pydantic import BaseModel, Field, ValidationError, ConfigDict
from flask import request, jsonify, g


def _validate(model, data):
    if hasattr(model, "model_validate"):
        return model.model_validate(data)
    return model.parse_obj(data)
def validate_json(model: type[BaseModel]):
    def dec(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            payload = request.get_json(silent=True)
            if payload is None:
                return jsonify({"error": "invalid_json", "message": "Expected JSON body"}), 422
            try:
                g.payload = _validate(model, payload)
            except ValidationError as e:
                return jsonify({"error": "validation_error", "details": e.errors()}), 422
            return fn(*args, **kwargs)
        return inner
    return dec
    # def dec(fn):
    #     @wraps(fn)
    #     def inner(*args, **kwargs):
    #         try:
    #             payload = request.get_json(silent=True)
    #             if payload is None:
    #                 return jsonify({"error": "invalid_json", "message": "Expected JSON body"}), 422
    #             try:
    #                 g.payload = _validate(model, payload)
    #             except ValidationError as e:
    #                 return jsonify({"error": "validation_error", "details": e.errors()}), 422
    #             return fn(*args, **kwargs)
    #     return inner
    # return dec

    # def dec(fn):
    #     @wraps(fn)
    #     def inner(*args, **kwargs):
    #         data = request.args.to_dict(flat=True)
    #         try:
    #             # Pydantic v2
    #             if hasattr(model, "model_validate"):
    #                 obj = model.model_validate(data)
    #             else:
    #                 # Pydantic v1
    #                 obj = model.parse_obj(data)
    #         # try:
    #         #     g.payload = model.model_validate(request.get_json(force=True))
    #         except ValidationError as e:
    #             return jsonify({"error": "validation_error", "details": e.errors()}), 422
    #         return fn(*args, **kwargs)
    #     return inner
    # return dec

def validate_query(model: type[BaseModel]):
    def dec(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            try:
                g.query = _validate(model, request.args.to_dict(flat=True))
            except ValidationError as e:
                return jsonify({"error": "validation_error", "details": e.errors()}), 422
            return fn(*args, **kwargs)
        return inner
    return dec
