import asyncio
import bs4
from bs4.element import Tag
from httpx import AsyncClient
from typing import List, Dict, Tuple

BASE_URL = "https://catalog.data.gov/dataset"



async def data_gov_initialize() -> Tuple[List[Dict[str, str]], int]:
    async with AsyncClient() as client:
        response = await client.get(BASE_URL)
        response.raise_for_status()
        html = bs4.BeautifulSoup(response.text, "html.parser")
        data = html.select_one("div.primary.col-md-9.col-xs-12 > section.module")
        module_content_list = data.select("div.module-content > ul.dataset-list.unstyled >li.dataset-item.has-organization")
        for module in module_content_list:
            module = module.select_one("div.dataset-content")
            dataset = {
                "title": module.select_one("h3.dataset-heading > a").text.strip(),
                "organization": module.select_one("div.organization-type-wrap > span.organization-type").attrs["data-organization-type"],
                "description": module.select_one("div.notes").text.strip(),
                "resources": [url.select_one("a")["href"] for url in module.select("ul.dataset-resources.unstyled > li")],
            }
        pagination_wrapper = html.select("div.pagination-wrapper > ul.pagination.justify-content-center > li.page-item")
        paginations = [int(pg.text.strip()) for pg in pagination_wrapper if pg.text.strip().isdigit()]
        max_page = max(paginations) + 1 if paginations else 1
        print(f"Total pages: {max_page}")
        return module_content_list, max_page


async def fetch_page(page_number: int = 1) -> list:
    async with AsyncClient() as client:
        response = await client.get(f"{BASE_URL}?page={page_number}")
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
        return datasets
    

async def main():
    pg1_datasets, max_page = await data_gov_initialize()
    for page in range(2, max_page + 1):
        pg_datasets = await fetch_page(page)
        pg1_datasets.extend(pg_datasets)
        print(f"Total datasets fetched: {len(pg1_datasets)}", end="\r")


if __name__ == "__main__":
    asyncio.run(main())