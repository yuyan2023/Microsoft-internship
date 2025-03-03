from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import random
import json
import os
import re
import mysql.connector
from datetime import datetime


class DoubanProxyScraper:
    """
    使用代理的豆瓣电影爬虫 - 增加代理支持，解决IP封禁问题
    """

    def __init__(self, json_file_path=None, use_proxy=True):
        self.json_file_path = json_file_path or f"douban_movies_{datetime.now().strftime('%Y%m%d')}.json"
        self.driver = None
        self.movies = []
        self.use_proxy = use_proxy

        # 代理配置 - 使用您提供的代理信息
        self.proxy_host = "dyn.horocn.com"
        self.proxy_port = "50000"
        self.proxy_user = "K8TP1824583505209241"
        self.proxy_pass = "ubgWRLuMmkFEZDhW"

        # 数据库配置
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "321227",
            "database": "spider_data"
        }

        # 加载现有数据（如果提供了文件路径且文件存在）
        if self.json_file_path and os.path.exists(self.json_file_path):
            self.load_existing_data()

    def load_existing_data(self):
        """加载现有JSON数据"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as f:
                self.movies = json.load(f)
            print(f"已加载 {len(self.movies)} 部电影基本信息")
        except Exception as e:
            print(f"加载数据出错: {e}")
            self.movies = []

    def setup_driver(self):
        """设置Selenium WebDriver，配置代理"""
        chrome_options = Options()

        # 配置代理
        if self.use_proxy:
            proxy_string = f"{self.proxy_host}:{self.proxy_port}"

            # 使用Chrome代理插件配置
            plugin_path = self.create_proxy_extension()
            if plugin_path:
                chrome_options.add_extension(plugin_path)
                print("已加载代理扩展")
            else:
                # 备用方法：直接设置代理
                chrome_options.add_argument(f'--proxy-server={proxy_string}')
                print(f"已设置代理服务器: {proxy_string}")

        # 禁用自动化控制特性
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # 常规设置
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")

        # 添加User-Agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ]
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')

        # 启动浏览器
        self.driver = webdriver.Chrome(options=chrome_options)

        # 设置隐式等待时间
        self.driver.implicitly_wait(10)

        # 修改WebDriver特征
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        })

    def create_proxy_extension(self):
        """创建Chrome代理扩展，支持用户名密码认证"""
        try:
            manifest_json = """
            {
                "version": "1.0.0",
                "manifest_version": 2,
                "name": "Chrome Proxy",
                "permissions": [
                    "proxy",
                    "tabs",
                    "unlimitedStorage",
                    "storage",
                    "webRequest",
                    "webRequestBlocking"
                ],
                "background": {
                    "scripts": ["background.js"]
                }
            }
            """

            background_js = """
            var config = {
                mode: "fixed_servers",
                rules: {
                    singleProxy: {
                        scheme: "http",
                        host: "%s",
                        port: parseInt(%s)
                    },
                    bypassList: ["localhost"]
                }
            };

            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "%s",
                        password: "%s"
                    }
                };
            }

            chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
            );
            """ % (self.proxy_host, self.proxy_port, self.proxy_user, self.proxy_pass)

            # 创建临时目录
            import tempfile
            import zipfile

            temp_dir = tempfile.mkdtemp()

            # 写入manifest.json和background.js
            with open(os.path.join(temp_dir, "manifest.json"), "w") as f:
                f.write(manifest_json)
            with open(os.path.join(temp_dir, "background.js"), "w") as f:
                f.write(background_js)

            # 创建zip文件
            plugin_path = os.path.join(temp_dir, "proxy_auth_ext.zip")
            with zipfile.ZipFile(plugin_path, "w") as zp:
                zp.write(os.path.join(temp_dir, "manifest.json"), "manifest.json")
                zp.write(os.path.join(temp_dir, "background.js"), "background.js")

            return plugin_path

        except Exception as e:
            print(f"创建代理扩展失败: {e}")
            return None

    def handle_captcha(self):
        """处理验证码"""
        captcha_patterns = [
            "验证码", "安全验证", "请输入验证码", "人机识别",
            "这是一个未经授权的请求", "检测到有异常请求"
        ]

        page_source = self.driver.page_source
        for pattern in captcha_patterns:
            if pattern in page_source:
                print(f"\n>>> 检测到验证码或安全页面: {pattern} <<<")

                # 保存页面源码以便分析
                with open(f"captcha_page_{int(time.time())}.html", "w", encoding="utf-8") as f:
                    f.write(page_source)

                print("请在打开的浏览器窗口中完成验证...")
                input("完成验证后按回车键继续...")
                time.sleep(3)
                return True

        return False

    def get_meta_content(self, property_name):
        """获取指定property的meta标签内容"""
        try:
            meta = self.driver.find_element(By.XPATH, f"//meta[@property='{property_name}']")
            return meta.get_attribute("content")
        except:
            return None

    def get_actors_from_meta(self):
        """从meta标签获取演员列表"""
        actors = []
        try:
            actor_metas = self.driver.find_elements(By.XPATH, "//meta[@property='video:actor']")
            for actor_meta in actor_metas:
                actor_name = actor_meta.get_attribute("content")
                if actor_name and actor_name.strip():
                    actors.append(actor_name.strip())
            return actors
        except:
            return []

    def scrape_movie_details(self, movie):
        """爬取电影详细信息"""
        url = movie.get('link')
        if not url:
            print(f"跳过无链接的电影: {movie.get('title')}")
            return movie

        print(f"正在爬取电影详情: {movie.get('title')} ({url})")

        try:
            self.driver.get(url)

            # 等待页面加载
            try:
                WebDriverWait(self.driver, 30).until(  # 增加等待时间到30秒
                    EC.presence_of_element_located((By.XPATH, "//meta[@property='og:title']"))
                )
            except TimeoutException:
                print(f"页面加载超时: {url}")
                # 检查是否有验证码
                if self.handle_captcha():
                    # 再次尝试等待页面加载
                    try:
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.XPATH, "//meta[@property='og:title']"))
                        )
                    except TimeoutException:
                        print(f"验证后页面仍未加载成功")
                        return movie
                else:
                    return movie

            # 处理验证码
            self.handle_captcha()

            # 获取演员信息
            actors = self.get_actors_from_meta()

            if actors:
                # 更新电影信息
                movie['actors'] = ','.join(actors[:15])  # 限制演员数量
                print(f"成功获取 {len(actors)} 名演员: {movie['actors']}")
            else:
                print(f"未找到演员信息")

            # 随机延迟
            time.sleep(random.uniform(2, 5))

            return movie

        except Exception as e:
            print(f"爬取电影详情出错 ({url}): {e}")
            return movie

    def crawl_page(self, page_index):
        """爬取电影列表页面"""
        start = page_index * 25
        url = f"https://movie.douban.com/top250?start={start}"

        print(f"\n开始爬取第 {page_index + 1} 页 (start={start})...")

        try:
            # 访问页面
            self.driver.get(url)

            # 等待页面加载
            try:
                WebDriverWait(self.driver, 30).until(  # 增加等待时间到30秒
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ol.grid_view"))
                )
            except TimeoutException:
                print(f"页面加载超时: {url}")
                # 检查是否有验证码
                if self.handle_captcha():
                    # 再次尝试等待页面加载
                    try:
                        WebDriverWait(self.driver, 15).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "ol.grid_view"))
                        )
                    except TimeoutException:
                        print(f"验证后页面仍未加载成功")
                        return False
                else:
                    return False

            # 模拟真实用户行为
            self.simulate_human_behavior()

            # 处理验证码
            self.handle_captcha()

            # 提取电影列表
            movie_items = self.driver.find_elements(By.CSS_SELECTOR, "div.item")
            if not movie_items:
                print(f"第 {page_index + 1} 页未找到电影条目，可能被反爬或页面结构变化")

                # 保存页面源码以便分析
                with open(f"page_{page_index}_source.html", "w", encoding="utf-8") as f:
                    f.write(self.driver.page_source)

                return False

            print(f"找到 {len(movie_items)} 个电影条目")

            # 处理每个电影条目
            page_movies = []
            for i, item in enumerate(movie_items):
                print(f"正在处理第 {i + 1}/{len(movie_items)} 个电影...")
                movie_data = self.extract_movie_info(item)
                if movie_data:
                    page_movies.append(movie_data)
                    print(f"已提取基本信息: {movie_data['title']} - 评分: {movie_data['rating']}")

                    # 随机暂停，模拟人类阅读行为
                    time.sleep(random.uniform(0.5, 1.5))

            # 所有电影都提取成功
            print(f"第 {page_index + 1} 页成功提取 {len(page_movies)} 部电影")

            # 获取演员信息
            enhanced_movies = []
            for i, movie in enumerate(page_movies):
                print(f"获取第 {i + 1}/{len(page_movies)} 部电影的演员信息...")
                enhanced_movie = self.scrape_movie_details(movie)
                enhanced_movies.append(enhanced_movie)

                # 较长随机延迟，避免过快请求
                time.sleep(random.uniform(3, 6))

            # 添加到总电影列表
            self.movies.extend(enhanced_movies)

            # 保存当前进度
            self.save_to_json()

            return True

        except Exception as e:
            print(f"爬取页面时出错: {e}")
            return False

    def simulate_human_behavior(self):
        """模拟人类浏览行为"""
        # 随机滚动
        for _ in range(random.randint(3, 6)):
            scroll_amount = random.randint(300, 800)
            self.driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(0.8, 2.0))

        # 随机暂停
        time.sleep(random.uniform(1.0, 3.0))

    def extract_movie_info(self, item):
        """从电影条目中提取信息"""
        try:
            # 标题
            title_elem = item.find_element(By.CSS_SELECTOR, "span.title")
            title = title_elem.text.strip()

            # 评分
            rating_elem = item.find_element(By.CSS_SELECTOR, "span.rating_num")
            rating = float(rating_elem.text.strip())

            # 评价人数
            rating_people_text = item.find_element(By.CSS_SELECTOR, "div.star span:last-child").text
            num_raters = int(re.search(r'(\d+)人评价', rating_people_text).group(1))

            # 名言
            quote = ""
            try:
                quote_elem = item.find_element(By.CSS_SELECTOR, "span.inq")
                quote = quote_elem.text.strip()
            except NoSuchElementException:
                pass

            # 详情链接
            link = item.find_element(By.CSS_SELECTOR, "div.hd a").get_attribute("href")

            # 信息部分 (导演、年份、类型等)
            info = item.find_element(By.CSS_SELECTOR, "div.bd p").text.strip()
            info_parts = info.split('\n')

            # 导演和主演
            first_line = info_parts[0].strip()
            director = ""
            if '导演:' in first_line:
                director = first_line.split('导演:')[1].split('主演:')[0].strip() if '主演:' in first_line else \
                first_line.split('导演:')[1].strip()

            # 年份、地区、类型
            second_line = info_parts[1].strip() if len(info_parts) > 1 else ""
            release_date = ""
            year_match = re.search(r'(\d{4})', second_line)
            if year_match:
                release_date = year_match.group(1)

            # 类型
            genres = []
            if '/' in second_line:
                parts = second_line.split('/')
                if len(parts) > 2:
                    genre_text = parts[2].strip()
                    genres = [g.strip() for g in genre_text.split(' ') if g.strip()]

            return {
                'title': title,
                'rating': rating,
                'num_raters': num_raters,
                'quote': quote,
                'director': director,
                'actors': "",  # 稍后将填充
                'release_date': release_date,
                'genres': ','.join(genres),
                'link': link
            }

        except Exception as e:
            print(f"提取电影信息时出错: {e}")
            return None

    def crawl_all(self):
        """爬取所有页面，直到达到目标数量"""
        try:
            # 设置WebDriver
            self.setup_driver()

            # 已爬取电影数量
            collected = len(self.movies)

            # 豆瓣电影Top250共10页
            max_pages = 10

            # 爬取策略：从第1页开始，逐页爬取
            for page in range(0, max_pages):
                print(f"\n准备爬取第 {page + 1} 页...")

                # 爬取一页
                success = self.crawl_page(page)

                if not success:
                    print(f"第 {page + 1} 页爬取失败，尝试重试...")
                    # 等待较长时间后重试一次
                    time.sleep(random.uniform(60, 120))  # 加长等待时间
                    success = self.crawl_page(page)

                    if not success:
                        print(f"第 {page + 1} 页重试仍然失败，跳过此页")

                # 页面间随机等待 (15-30秒)
                wait_time = random.uniform(15, 30)
                print(f"等待 {wait_time:.2f} 秒后继续下一页...")
                time.sleep(wait_time)

                # 每爬取2页后，较长时间休息一次
                if (page + 1) % 2 == 0 and page < max_pages - 1:
                    long_break = random.uniform(120, 240)  # 2-4分钟
                    print(f"完成2页，进行较长休息 {long_break:.2f} 秒...")
                    time.sleep(long_break)

            # 保存到数据库
            self.save_to_database()

            print(f"爬取完成，共获取 {len(self.movies)} 部电影")

        except Exception as e:
            print(f"爬取过程中出错: {e}")
        finally:
            # 保存当前进度
            self.save_to_json()

            # 关闭浏览器
            if self.driver:
                self.driver.quit()

    def save_to_json(self):
        """保存电影数据到JSON文件"""
        try:
            with open(self.json_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.movies, f, ensure_ascii=False, indent=4)
            print(f"数据已保存到 {self.json_file_path}")
        except Exception as e:
            print(f"保存JSON文件时出错: {e}")

    def save_to_database(self):
        """保存电影数据到MySQL数据库"""
        if not self.movies:
            print("没有电影数据可保存")
            return

        connection = None
        try:
            connection = mysql.connector.connect(**self.db_config)
            if connection.is_connected():
                print("已连接到数据库")

                # 创建游标
                cursor = connection.cursor()

                # 检查表是否存在，如果不存在则创建
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS movies (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        rating FLOAT,
                        num_raters INT,
                        quote TEXT,
                        director VARCHAR(255),
                        actors TEXT,
                        release_date VARCHAR(50),
                        genres VARCHAR(255),
                        link VARCHAR(255),
                        UNIQUE INDEX idx_title (title)
                    ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
                """)

                # 插入或更新电影数据
                sql = """
                    INSERT INTO movies 
                    (title, rating, num_raters, quote, director, actors, release_date, genres, link) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    rating = VALUES(rating),
                    num_raters = VALUES(num_raters),
                    quote = VALUES(quote),
                    director = VALUES(director),
                    actors = VALUES(actors),
                    release_date = VALUES(release_date),
                    genres = VALUES(genres),
                    link = VALUES(link)
                """

                success_count = 0
                for movie in self.movies:
                    try:
                        params = (
                            movie.get('title', ''),
                            movie.get('rating', 0.0),
                            movie.get('num_raters', 0),
                            movie.get('quote', ''),
                            movie.get('director', ''),
                            movie.get('actors', ''),
                            movie.get('release_date', ''),
                            movie.get('genres', ''),
                            movie.get('link', '')
                        )

                        cursor.execute(sql, params)
                        connection.commit()
                        success_count += 1

                        if success_count % 10 == 0:
                            print(f"已保存 {success_count} 部电影")

                    except Exception as e:
                        print(f"保存电影 {movie.get('title')} 时出错: {e}")
                        connection.rollback()

                print(f"成功保存 {success_count}/{len(self.movies)} 部电影")

        except Exception as e:
            print(f"数据库操作失败: {e}")
        finally:
            if connection and connection.is_connected():
                cursor.close()
                connection.close()
                print("数据库连接已关闭")


class DoubanSinglePageScraper(DoubanProxyScraper):
    """
    单页爬虫 - 只爬取一个特定页面，便于断点续爬
    """

    def scrape_page(self, page_index):
        """爬取单个页面"""
        try:
            # 设置WebDriver
            self.setup_driver()

            # 爬取页面
            success = self.crawl_page(page_index)

            if not success:
                print(f"第 {page_index + 1} 页爬取失败，尝试重试...")
                # 等待较长时间后重试一次
                time.sleep(random.uniform(60, 120))
                success = self.crawl_page(page_index)

                if not success:
                    print(f"第 {page_index + 1} 页重试仍然失败")

            # 保存到数据库
            self.save_to_database()

        except Exception as e:
            print(f"爬取过程中出错: {e}")
        finally:
            # 保存当前进度
            self.save_to_json()

            # 关闭浏览器
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    # 选择爬取模式
    print("请选择爬取模式:")
    print("1. 爬取所有页面")
    print("2. 爬取单个页面")
    mode = input("请输入选项 (1/2): ").strip()

    # JSON文件路径
    json_file_path = "douban_movies_20250221.json"

    if mode == "1":
        # 创建爬虫实例并开始爬取所有页面
        scraper = DoubanProxyScraper(json_file_path)
        scraper.crawl_all()
    elif mode == "2":
        # 爬取单个页面
        page = int(input("请输入要爬取的页码(0-9): ").strip())
        scraper = DoubanSinglePageScraper(json_file_path)
        scraper.scrape_page(page)
    else:
        print("无效选项")