from flask import Flask, request, jsonify
import asyncio
import json
import re
from datetime import timedelta
from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import PlaywrightCrawler
from crawlee.router import Router
from crawlee.crawlers import PlaywrightCrawlingContext
from playwright.async_api import Page
import os

app = Flask(__name__)

# Router cho crawler
router = Router[PlaywrightCrawlingContext]()

# Hàm helper để trích xuất link video
async def extract_video_links(page: Page) -> list[Request]:
    """Trích xuất tất cả link video đã load từ trang."""
    links = []
    
    selectors = [
        '[data-e2e="user-post-item"] a',
        '[data-e2e="user-post-item-list"] a',
        'div[data-e2e="user-post-item"] a',
        'a[href*="/video/"]',
    ]
    
    for selector in selectors:
        try:
            posts = await page.query_selector_all(selector)
            for post in posts:
                post_link = await post.get_attribute('href')
                if post_link and '/video/' in post_link:
                    if post_link.startswith('/'):
                        post_link = f"https://www.tiktok.com{post_link}"
                    links.append(Request.from_url(post_link, label='video'))
        except Exception as e:
            continue
    
    # Loại bỏ duplicate
    unique_links = []
    seen_urls = set()
    for link in links:
        if link.url not in seen_urls:
            unique_links.append(link)
            seen_urls.add(link.url)
    
    return unique_links

@router.default_handler
async def default_handler(context: PlaywrightCrawlingContext) -> None:
    """Xử lý request tự động phát hiện loại URL."""
    url = context.request.url
    
    if '/video/' in url:
        await scrape_single_video(context)
    else:
        await scrape_user_profile(context)

async def scrape_single_video(context: PlaywrightCrawlingContext) -> None:
    """Cào dữ liệu từ một video cụ thể."""
    try:
        await context.page.wait_for_load_state('domcontentloaded')
        await context.page.wait_for_timeout(3000)
        
        json_element = await context.page.query_selector('#__UNIVERSAL_DATA_FOR_REHYDRATION__')
        if json_element:
            text_data = await json_element.text_content()
            json_data = json.loads(text_data)
            
            data = None
            possible_paths = [
                ['__DEFAULT_SCOPE__', 'webapp.video-detail', 'itemInfo', 'itemStruct'],
                ['__DEFAULT_SCOPE__', 'webapp.video-detail', 'itemInfo'],
                ['__DEFAULT_SCOPE__', 'webapp.video-detail'],
                ['__DEFAULT_SCOPE__', 'seo.abtest'],
            ]
            
            for path in possible_paths:
                try:
                    temp_data = json_data
                    for key in path:
                        temp_data = temp_data[key]
                    
                    if 'itemStruct' in temp_data:
                        data = temp_data['itemStruct']
                        break
                    elif 'itemInfo' in temp_data and 'itemStruct' in temp_data['itemInfo']:
                        data = temp_data['itemInfo']['itemStruct']
                        break
                    elif 'id' in temp_data:
                        data = temp_data
                        break
                except (KeyError, TypeError):
                    continue
            
            if not data:
                raise RuntimeError('Could not find video data')
            
            result_item = {
                'video_id': data.get('id', 'unknown'),
                'video_url': context.request.url,
                'author': {
                    'nickname': data.get('author', {}).get('nickname', 'unknown'),
                    'id': data.get('author', {}).get('id', 'unknown'),
                    'handle': data.get('author', {}).get('uniqueId', 'unknown'),
                    'signature': data.get('author', {}).get('signature', ''),
                    'followers': data.get('authorStats', {}).get('followerCount', 0),
                    'following': data.get('authorStats', {}).get('followingCount', 0),
                    'hearts': data.get('authorStats', {}).get('heart', 0),
                    'videos': data.get('authorStats', {}).get('videoCount', 0),
                },
                'description': data.get('desc', ''),
                'tags': [item.get('hashtagName', '') for item in data.get('textExtra', []) if item.get('hashtagName')],
                'hearts': data.get('stats', {}).get('diggCount', 0),
                'shares': data.get('stats', {}).get('shareCount', 0),
                'comments': data.get('stats', {}).get('commentCount', 0),
                'plays': data.get('stats', {}).get('playCount', 0),
                'created_time': data.get('createTime', ''),
                'video_duration': data.get('video', {}).get('duration', 0),
            }
            
            await context.push_data(result_item)
        else:
            raise RuntimeError('No JSON data element found')
    except Exception as e:
        raise

async def scrape_user_profile(context: PlaywrightCrawlingContext) -> None:
    """Cào tất cả video từ trang profile user."""
    limit = context.request.user_data.get('limit', 10)
    
    await context.page.wait_for_load_state('domcontentloaded')
    await context.page.wait_for_timeout(3000)
    
    check_locator = context.page.locator('[data-e2e="user-post-item"], main button').first
    await check_locator.wait_for(timeout=10000)
    
    if button := await context.page.query_selector('main button'):
        await button.click()
        await context.page.wait_for_timeout(2000)
    
    collected_links = set()
    max_scrolls = limit * 2
    scroll_count = 0
    no_new_links_count = 0
    
    while len(collected_links) < limit and scroll_count < max_scrolls:
        current_links = await extract_video_links(context.page)
        previous_count = len(collected_links)
        
        for link in current_links:
            collected_links.add(link.url)
        
        current_count = len(collected_links)
        
        if current_count == previous_count:
            no_new_links_count += 1
        else:
            no_new_links_count = 0
        
        if no_new_links_count >= 3:
            break
        
        await context.page.keyboard.press('PageDown')
        await context.page.wait_for_timeout(2000)
        
        if scroll_count % 5 == 0:
            await context.page.click('body')
            await context.page.wait_for_timeout(1000)
        
        scroll_count += 1
    
    requests = []
    for url in list(collected_links)[:limit]:
        requests.append(Request.from_url(url, label='video'))
    
    if not requests:
        raise RuntimeError('No video links found')
    
    await context.add_requests(requests)

@router.handler(label='video')
async def video_handler(context: PlaywrightCrawlingContext) -> None:
    """Xử lý request với label 'video' từ user profile."""
    await scrape_single_video(context)

def validate_tiktok_url(url):
    """Kiểm tra URL TikTok hợp lệ."""
    patterns = [
        r'https?://(?:www\.)?tiktok\.com/@[\w.-]+/?$',  # Profile URL
        r'https?://(?:www\.)?tiktok\.com/@[\w.-]+/video/\d+/?$',  # Video URL
        r'https?://vm\.tiktok\.com/[\w]+/?$',  # Short URL
    ]
    return any(re.match(pattern, url) for pattern in patterns)

async def scrape_tiktok_data(url: str, limit: int = 10):
    """Hàm chính để cào dữ liệu TikTok."""
    if not validate_tiktok_url(url):
        raise ValueError("Invalid TikTok URL")
    
    # Tạo crawler
    crawler = PlaywrightCrawler(
        concurrency_settings=ConcurrencySettings(
            max_tasks_per_minute=30,
            desired_concurrency=2,
        ),
        request_handler=router,
        headless=True,
        max_requests_per_crawl=50,
        request_handler_timeout=timedelta(seconds=180),
        browser_type='firefox',
        browser_new_context_options={'permissions': []},
    )
    
    # Chạy crawler
    await crawler.run([
        Request.from_url(url, user_data={'limit': limit}),
    ])
    
    # Lấy dữ liệu từ dataset
    data = await crawler.get_data()
    return data.to_dict('records') if data is not None else []

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({"status": "healthy", "message": "TikTok Scraper API is running"})

@app.route('/scrape', methods=['POST'])
def scrape_endpoint():
    """API endpoint để cào dữ liệu TikTok."""
    try:
        data = request.get_json()
        
        if not data or 'url' not in data:
            return jsonify({
                "error": "Missing required parameter: url",
                "example": {
                    "url": "https://www.tiktok.com/@username",
                    "limit": 10
                }
            }), 400
        
        url = data['url']
        limit = data.get('limit', 10)
        
        # Validate limit
        if not isinstance(limit, int) or limit < 1 or limit > 50:
            return jsonify({
                "error": "Limit must be an integer between 1 and 50"
            }), 400
        
        # Validate URL
        if not validate_tiktok_url(url):
            return jsonify({
                "error": "Invalid TikTok URL. Supported formats:",
                "formats": [
                    "https://www.tiktok.com/@username",
                    "https://www.tiktok.com/@username/video/1234567890",
                    "https://vm.tiktok.com/abc123"
                ]
            }), 400
        
        # Chạy scraper
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(scrape_tiktok_data(url, limit))
            return jsonify({
                "success": True,
                "data": result,
                "count": len(result),
                "url": url,
                "limit": limit
            })
        finally:
            loop.close()
            
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/scrape', methods=['GET'])
def scrape_get_endpoint():
    """API endpoint GET để cào dữ liệu TikTok với URL params."""
    try:
        url = request.args.get('url')
        limit = request.args.get('limit', 10)
        
        if not url:
            return jsonify({
                "error": "Missing required parameter: url",
                "example": "/scrape?url=https://www.tiktok.com/@username&limit=10"
            }), 400
        
        try:
            limit = int(limit)
        except ValueError:
            return jsonify({"error": "Limit must be an integer"}), 400
        
        if limit < 1 or limit > 50:
            return jsonify({
                "error": "Limit must be between 1 and 50"
            }), 400
        
        if not validate_tiktok_url(url):
            return jsonify({
                "error": "Invalid TikTok URL"
            }), 400
        
        # Chạy scraper
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(scrape_tiktok_data(url, limit))
            return jsonify({
                "success": True,
                "data": result,
                "count": len(result),
                "url": url,
                "limit": limit
            })
        finally:
            loop.close()
            
    except Exception as e:
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/', methods=['GET'])
def home():
    """API documentation."""
    return jsonify({
        "name": "TikTok Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "GET /": "API documentation",
            "GET /health": "Health check",
            "POST /scrape": "Scrape TikTok data with JSON body",
            "GET /scrape": "Scrape TikTok data with URL parameters"
        },
        "usage": {
            "POST /scrape": {
                "body": {
                    "url": "https://www.tiktok.com/@username",
                    "limit": 10
                }
            },
            "GET /scrape": "?url=https://www.tiktok.com/@username&limit=10"
        },
        "supported_urls": [
            "https://www.tiktok.com/@username (user profile)",
            "https://www.tiktok.com/@username/video/1234567890 (single video)",
            "https://vm.tiktok.com/abc123 (short link)"
        ]
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)