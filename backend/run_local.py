import sys
sys.path.insert(0, 'backend')

from app import main
import uvicorn

if __name__ == '__main__':
    uvicorn.run(main.app, host='127.0.0.1', port=8000)
