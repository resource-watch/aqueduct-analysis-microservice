import asyncio
import aiohttp

api_urls = ["http://httpbin.org/get", "http://httpbin.org/get", "http://httpbin.org/get"]

async def fetch_geocode_api(session, url):
    async with session.get(url) as response:
        assert response.status == 200
        return await response.read()

async def run_geocode(urls):
    tasks = []

    # Fetch all responses within one Client session,
    # keep connection alive for all requests.
    async with aiohttp.ClientSession() as session:
        for i in range(len(urls)):
            task = asyncio.ensure_future(fetch_geocode_api(session, urls[i]))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        # you now have all response bodies in this variable
        serialize_output(responses)

def serialize_output(result):
    
    print(result)

loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run_geocode(api_urls))
loop.run_until_complete(future)