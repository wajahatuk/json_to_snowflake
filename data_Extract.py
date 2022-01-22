import aiohttp
import asyncio
import json
import os
import errno
from aiohttp import ClientSession


URLS = ["http://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/users",
        "http://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/messages"]
data_dir = 'data/'
extension = '.json'

async def get_data_async(URL, session):
    """"""
    url = URL
    try:
        response = await session.request(method='GET', url=url)
        response.raise_for_status()
        print(f"Response status ({url}): {response.status}")
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error ocurred: {err}")
    response_json = await response.json()
    return response_json

def write_data(filename,data):

    directory = data_dir+filename+'/'
    if not os.path.exists(os.path.dirname(directory)):
        try:
            os.makedirs(os.path.dirname(directory))
        except OSError as exc: 
            if exc.errno != errno.EEXIST:
                raise
    with open(data_dir+filename+'/'+filename+extension, 'w') as outfile:
        outfile.write(data)

async def run_program(URL, session):
    """"""
    try:
        response = await get_data_async(URL, session)
        write_data(URL.split('/')[-1],json.dumps(response, indent=2))
        print(f"Response: {json.dumps(response, indent=2)}")
    except Exception as err:
        print(f"Exception occured: {err}")
        pass

async def program():
    async with ClientSession() as session:
        await asyncio.gather(*[run_program(url, session) for url in URLS])

def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(program())
    loop.close()

if __name__=='__main__':
    main()