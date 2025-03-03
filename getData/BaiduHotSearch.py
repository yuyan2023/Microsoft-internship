import re
from tools.crawlerTool import CrawlerTool
from tools.mySqlHelper import MySqlHelper

class BaiduHotSearchScraper(CrawlerTool):
    """Class for scraping Baidu's Hot Search page"""

    def parse_data(self, soup, limit):
        hot_items = soup.find_all("div", class_="category-wrap_iQLoo", limit=limit)
        hot_data = []
        for item in hot_items:
            title_tag = item.find("div", class_="c-single-text-ellipsis")
            title = title_tag.get_text(strip=True) if title_tag else "未知标题"
            content_tag = item.find("div", class_=re.compile(r"hot-desc_.*"))
            content = content_tag.get_text(strip=True) if content_tag else "暂无内容"
            hot_data.append((title, content))
        return hot_data

if __name__ == "__main__":
    # 百度热搜页面URL和请求头
    url = "https://top.baidu.com/board?tab=realtime"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    }

    # 爬取百度热搜数据
    scraper = BaiduHotSearchScraper(url, headers)
    hot_items = scraper.fetch_data(limit=10)

    if hot_items:
        # 将爬取的数据保存到数据库
        db_config = {
            "host": "localhost",
            "user": "root",
            "password": "321227",
            "database": "spider_data"
        }
        db_saver = MySqlHelper(db_config)
        # 可以自定义表名
        db_saver.save_data(hot_items, table="baidu_hot_search")  # 这里你可以根据需要更改表名
    else:
        print("未获取到数据")
