import asyncio, bs4, json, time, random
from httpx import AsyncClient, HTTPStatusError, ReadError
from typing import List, Dict, Tuple


BASE_URL = "https://catalog.data.gov/dataset"
SEMAPHORE = asyncio.Semaphore(10)  # Limit concurrent scraping
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

PAUSE = asyncio.Event()
PAUSE.set()  # start unpaused

CLIENT = AsyncClient(headers=HEADERS)

pg_datasets = []
total_to_be_scrapped = 0

def random_user_agent():
    # OS options
    os_options = [
        # Windows
        "Windows NT 10.0; Win64; x64",
        "Windows NT 11.0; Win64; x64",
        # macOS
        "Macintosh; Intel Mac OS X 10_15_7",
        "Macintosh; Intel Mac OS X 13_6_0",
        # Linux
        "X11; Linux x86_64"
    ]

    os_choice = random.choice(os_options)

    # Browser options
    browsers = ["Chrome", "Edg", "Firefox", "Brave"]
    browser = random.choice(browsers)

    # Generate version numbers
    major = random.choice(range(120, 127))
    minor = random.randint(0, 9999)
    build = random.randint(0, 200)
    patch = random.randint(0, 200)

    if browser == "Chrome":
        ua = f"Mozilla/5.0 ({os_choice}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.{minor}.{build}.{patch} Safari/537.36"
    elif browser == "Edg":
        ua = f"Mozilla/5.0 ({os_choice}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.{minor}.{build}.{patch} Edg/{major}.{minor}.{build}.{patch}"
    elif browser == "Firefox":
        # Firefox format: Mozilla/5.0 (<OS>) Gecko/20100101 Firefox/<major>.0
        ff_major = random.choice(range(115, 125))
        ua = f"Mozilla/5.0 ({os_choice}; rv:{ff_major}.0) Gecko/20100101 Firefox/{ff_major}.0"
    elif browser == "Brave":
        # Brave uses Chrome UA base but can add "Brave/<version>" sometimes in extensions
        ua = f"Mozilla/5.0 ({os_choice}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.{minor}.{build}.{patch} Safari/537.36 Brave/{major}.{minor}.{build}.{patch}"

    return ua



async def re_initate_client_and_header():
    global CLIENT, HEADERS
    await CLIENT.aclose()
    HEADERS["User-Agent"] = random_user_agent()
    CLIENT = AsyncClient(headers=HEADERS)

async def data_gov_initialize() -> Tuple[List[Dict[str, str]], int]:
    response = await CLIENT.get(BASE_URL)
    response.raise_for_status()
    html = bs4.BeautifulSoup(response.text, "html.parser")
    data = html.select_one("div.primary.col-md-9.col-xs-12 > section.module")
    module_content_list = data.select("div.module-content > ul.dataset-list.unstyled >li.dataset-item.has-organization")
    datasets = []
    for module in module_content_list:
        module = module.select_one("div.dataset-content")
        dataset = {
            "title": module.select_one("h3.dataset-heading > a").text.strip(),
            "organization": module.select_one("div.organization-type-wrap > span.organization-type").attrs["data-organization-type"],
            "description": module.select_one("div.notes").text.strip(),
            "resources": [url.select_one("a")["href"] for url in module.select("ul.dataset-resources.unstyled > li")],
        }
        datasets.append(dataset)
    pagination_wrapper = html.select("div.pagination-wrapper > ul.pagination.justify-content-center > li.page-item")
    paginations = [int(pg.text.strip()) for pg in pagination_wrapper if pg.text.strip().isdigit()]
    max_page = max(paginations) + 1 if paginations else 1
    print(f"Total pages: {max_page:,}")
    return datasets, max_page


async def fetch_page(page_number: int = 1) -> list:
    try:
        async with SEMAPHORE:
            await PAUSE.wait()
            await asyncio.sleep(random.uniform(0.2, 0.8))  # Random delay between requests
            response = await CLIENT.get(f"{BASE_URL}?page={page_number}")
            response.raise_for_status()
            html = bs4.BeautifulSoup(response.text, "html.parser")
            data = html.select_one("div.primary.col-md-9.col-xs-12 > section.module")
            module_content_list = data.select("div.module-content > ul.dataset-list.unstyled >li.dataset-item.has-organization")
            datasets = []
            for module in module_content_list:
                module = module.select_one("div.dataset-content")
                dataset = {
                    "title": module.select_one("h3.dataset-heading > a").text.strip(),
                    "organization": module.select_one("div.organization-type-wrap > span.organization-type").attrs["data-organization-type"] if module.select_one("div.organization-type-wrap > span.organization-type") else "Unknown",
                    "description": module.select_one("div.notes").text.strip(),
                    "resources": [url.select_one("a")["href"] for url in module.select("ul.dataset-resources.unstyled > li")],
                }
                datasets.append(dataset)
        return datasets
    except Exception as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        print(f"Error {status} | Page {page_number}{' ' * 50}", end="\r", flush=True)

        # trigger pause by one coroutine
        if PAUSE.is_set() and status in (403, 429):
            PAUSE.clear()  # stop all requests
            wait_seconds = 80
            await re_initate_client_and_header()
            print(f"Pausing all tasks for {wait_seconds}s...  | Last Progress: {len(pg_datasets):,} of {total_to_be_scrapped:,}{' ' * 50}", end="\r", flush=True)
            await asyncio.sleep(wait_seconds)  # global backoff
            save_dataset_to_json(pg_datasets) # save current progress
            PAUSE.set()  # resume all tasks
            print(f"Resuming requests...{' ' * 50}", end="\r", flush=True)

        # retry this page after pause
        return await fetch_page(page_number)

def save_dataset_to_json(datasets, premature=True):
    with open("data_gov_datasets.json", "w", encoding="utf-8") as file:
        json.dump(datasets, file, indent=4, ensure_ascii=False)
    if premature:
        print(f"Datasets saved to data_gov_datasets.json | Total datasets: {len(datasets):,}")



async def main():
    global pg_datasets
    start = time.time()
    pg_datasets, max_page = await data_gov_initialize()
    total_to_be_scrapped = max_page * 20
    count = 2

    tasks = [asyncio.create_task(fetch_page(page)) for page in range(count, max_page + 1)]

    for coroutine in asyncio.as_completed(tasks):
        datasets = await coroutine
        pg_datasets.extend(datasets)
        print(f"Progress: {len(pg_datasets):,} of {total_to_be_scrapped:,}{' ' * 50}", end="\r", flush=True)

    print(f"\nTotal datasets scrapped: {len(pg_datasets):,} | Time taken: {time.time() - start:.2f} seconds{' ' * 50}")
    save_dataset_to_json(pg_datasets, premature=False)

    # close the client session
    await CLIENT.aclose()


if __name__ == "__main__":
    asyncio.run(main())