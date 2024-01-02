## Setup

Get the code and install dependencies:
```bash
git clone git@github.com:TCatshoek/fastapi-postgres-sse.git
cd fastapi-postgres-sse
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Also set up postgres, using docker for a quick temporary database is easiest:
```bash
docker run --rm -d \
    --name tmp-postgres \
    -e POSTGRES_PASSWORD=password \
    -p 5432:5432 \
    postgres
```

## Run
```bash
uvicorn main:app
```

Then, to listen to updates:
```bash
curl -N http://localhost:8000/updates
```

And sending messages:
```bash
 curl -X POST http://localhost:8000/ -d "Hi!"
```

## Cleanup
```bash
docker stop tmp-postgres
```