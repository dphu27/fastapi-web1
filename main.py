from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import importlib.util
import os

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "titles": ["Bài 1.1", "Bài 1.2"]
    })


@app.get("/exercise/{name}", response_class=HTMLResponse)
async def show_exercise(request: Request, name: str):
    mapping = {
        "Bài 1.1": "ex_1",
        "Bài 1.2": "ex_2"
    }

    filename = mapping.get(name)
    code = ""

    if filename:
        path = f"exercises/{filename}.py"
        try:
            with open(path, "r", encoding="utf-8") as f:
                code = f.read()
        except Exception as e:
            code = f"Lỗi khi đọc file: {e}"
    else:
        code = "Không tìm thấy bài tập."

    return templates.TemplateResponse("exercise.html", {
        "request": request,
        "name": name,
        "code": code
    })


@app.post("/run/{name}", response_class=HTMLResponse)
async def run_exercise(request: Request, name: str):
    mapping = {
        "Bài 1.1": "ex_1",
        "Bài 1.2": "ex_2"
    }

    result = ""
    try:
        file_name = mapping.get(name)
        if not file_name:
            result = f"Không tìm thấy bài: {name}"
        else:
            path = f"exercises/{file_name}.py"
            spec = importlib.util.spec_from_file_location(file_name, path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            result = module.run()
    except Exception as e:
        result = f"Lỗi khi chạy bài {name}: {str(e)}"

    return templates.TemplateResponse("result.html", {
        "request": request,
        "name": name,
        "result": result
    })
