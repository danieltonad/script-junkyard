import asyncio, bs4, json, time, random
from bs4.element import Tag
from httpx import AsyncClient, HTTPStatusError
from typing import List, Dict, Tuple


BASE_URL = "https://catalog.data.gov/dataset"
SEMAPHORE = asyncio.Semaphore(5)  # Limit concurrent scraping
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/126.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}
CLIENT = AsyncClient()

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
    print(f"Total pages: {max_page:,} | Data items found: {len(datasets)}")
    return datasets, max_page


async def fetch_page(page_number: int = 1) -> list:
    try:
        async with SEMAPHORE:
            await asyncio.sleep(random.uniform(0.2, 0.8))  # Random delay between requests
            response = await CLIENT.get(f"{BASE_URL}?page={page_number}", headers=HEADERS)
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
                # print(f"Page {page_number}: Scrapped {len(datasets):,} items", end="\r")
        return datasets
    except HTTPStatusError as e:
        if e.response.status_code in (403, 429):
            print(f"Rate limited or forbidden: {e.response.status_code} | Page {page_number:,}") 
        await asyncio.sleep(5)  # Wait before retrying
        return await fetch_page(page_number)  # Retry fetching the page

def save_dataset_to_json(datasets):
    with open("data_gov_datasets.json", "w", encoding="utf-8") as file:
        json.dump(datasets, file, indent=4, ensure_ascii=False)
    print(f"Datasets saved to data_gov_datasets.json | Total datasets: {len(datasets):,}")
    


async def main():
    start = time.time()
    pg_datasets, max_page = await data_gov_initialize()
    total_to_be_scrapped = max_page * 20
    count = 2

    # Prepare tasks for all remaining pages
    tasks = [asyncio.create_task(fetch_page(page)) for page in range(count, max_page + 1)]

    for coroutine in asyncio.as_completed(tasks):
        datasets = await coroutine
        pg_datasets.extend(datasets)
        print(f"Scrapped  {len(pg_datasets):,} of {total_to_be_scrapped:,}", end="\r")

    print(f"\nTotal datasets scrapped: {len(pg_datasets):,} | Time taken: {time.time() - start:.2f} seconds")
    save_dataset_to_json(pg_datasets)


if __name__ == "__main__":
    asyncio.run(main())
    # print(asyncio.run(fetch_page(4)))