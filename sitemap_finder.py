import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class SitemapFinder:
    def __init__(self, domain):
        self.domain = domain
        self.base_url = f"https://{domain}"
        self.found_sitemaps = set()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def check_url(self, url, timeout=10):
        """Check if a URL exists and contains sitemap data"""
        try:
            response = self.session.get(url, timeout=timeout, allow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '').lower()
                content = response.text.lower()
                
                # Check if it's likely a sitemap
                if any(indicator in content for indicator in ['<urlset', '<sitemapindex', '<?xml']):
                    return True, response.text
                elif 'xml' in content_type:
                    return True, response.text
            return False, None
        except Exception as e:
            return False, None
    
    def check_robots_txt(self):
        """Parse robots.txt for sitemap declarations"""
        print(f"Checking robots.txt...")
        robots_url = f"{self.base_url}/robots.txt"
        exists, content = self.check_url(robots_url)
        
        if exists and content:
            sitemap_pattern = re.compile(r'Sitemap:\s*(.+)', re.IGNORECASE)
            matches = sitemap_pattern.findall(content)
            for match in matches:
                sitemap_url = match.strip()
                if not sitemap_url.startswith('http'):
                    sitemap_url = urljoin(self.base_url, sitemap_url)
                self.found_sitemaps.add(sitemap_url)
                print(f"  Found in robots.txt: {sitemap_url}")
    
    def check_common_patterns(self):
        """Check common sitemap URL patterns"""
        print(f"\nChecking common sitemap patterns...")
        
        common_patterns = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap-index.xml',
            '/sitemap.xml.gz',
            '/sitemap1.xml',
            '/sitemap0.xml',
            '/sitemaps/sitemap.xml',
            '/sitemap/sitemap.xml',
            '/sitemap/index.xml',
            '/sitemap-main.xml',
            '/sitemap_main.xml',
            '/sitemap-pages.xml',
            '/sitemap_pages.xml',
            '/sitemap-posts.xml',
            '/sitemap_posts.xml',
            '/sitemap-products.xml',
            '/sitemap_products.xml',
            '/sitemap-categories.xml',
            '/sitemap_categories.xml',
            '/sitemap-tags.xml',
            '/sitemap_tags.xml',
            '/sitemap-blog.xml',
            '/sitemap_blog.xml',
            '/sitemap-news.xml',
            '/sitemap_news.xml',
            '/sitemap-video.xml',
            '/sitemap_video.xml',
            '/sitemap-image.xml',
            '/sitemap_image.xml',
            '/sitemap-mobile.xml',
            '/sitemap_mobile.xml',
            '/wp-sitemap.xml',  # WordPress
            '/news-sitemap.xml',
            '/video-sitemap.xml',
            '/image-sitemap.xml',
            '/mobile-sitemap.xml',
            '/page-sitemap.xml',
            '/post-sitemap.xml',
            '/category-sitemap.xml',
            '/product-sitemap.xml',
            '/yoast-sitemap.xml',
            '/sitemap_index.php',
            '/sitemap.php',
            '/sitemap.aspx',
            '/sitemap.ashx',
            '/vp-sitemap.xml',
            '/sitemap-misc.xml',
            '/sitemap-authors.xml',
            '/sitemap-pt-post.xml',
            '/sitemap-pt-page.xml',
            '/sitemap-tax-category.xml',
            '/feed/sitemap.xml',
            '/sitemap/sitemap-index.xml',
            '/.sitemap.xml',
            '/.sitemap-index.xml'
        ]
        
        # Check with and without www
        domains_to_check = [self.base_url]
        if 'www.' not in self.domain:
            domains_to_check.append(f"https://www.{self.domain}")
        
        urls_to_check = []
        for domain in domains_to_check:
            for pattern in common_patterns:
                urls_to_check.append(domain + pattern)
        
        # Check URLs in parallel
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(self.check_url, url, 5): url for url in urls_to_check}
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    exists, content = future.result()
                    if exists:
                        self.found_sitemaps.add(url)
                        print(f"  Found: {url}")
                except Exception as e:
                    pass
    
    def check_homepage_for_sitemap_links(self):
        """Check homepage HTML for sitemap links"""
        print(f"\nChecking homepage for sitemap links...")
        
        exists, content = self.check_url(self.base_url)
        if exists and content:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Check all links
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'sitemap' in href.lower():
                    full_url = urljoin(self.base_url, href)
                    if full_url not in self.found_sitemaps:
                        exists, _ = self.check_url(full_url, 5)
                        if exists:
                            self.found_sitemaps.add(full_url)
                            print(f"  Found in HTML: {full_url}")
            
            # Check meta tags
            for meta in soup.find_all('meta'):
                if meta.get('name', '').lower() == 'sitemap' or meta.get('property', '').lower() == 'sitemap':
                    content = meta.get('content', '')
                    if content:
                        full_url = urljoin(self.base_url, content)
                        if full_url not in self.found_sitemaps:
                            exists, _ = self.check_url(full_url, 5)
                            if exists:
                                self.found_sitemaps.add(full_url)
                                print(f"  Found in meta tag: {full_url}")
    
    def check_well_known_locations(self):
        """Check .well-known directory"""
        print(f"\nChecking .well-known directory...")
        well_known_urls = [
            '/.well-known/sitemap.xml',
            '/.well-known/sitemaps/sitemap.xml'
        ]
        
        for pattern in well_known_urls:
            url = self.base_url + pattern
            exists, _ = self.check_url(url, 5)
            if exists:
                self.found_sitemaps.add(url)
                print(f"  Found: {url}")
    
    def check_cms_specific_locations(self):
        """Check CMS-specific sitemap locations"""
        print(f"\nChecking CMS-specific locations...")
        
        cms_patterns = {
            'WordPress': [
                '/wp-sitemap.xml',
                '/wp-sitemap-posts-post-1.xml',
                '/wp-sitemap-posts-page-1.xml',
                '/wp-sitemap-taxonomies-category-1.xml',
                '/wp-sitemap-taxonomies-post_tag-1.xml',
                '/wp-sitemap-users-1.xml'
            ],
            'Drupal': [
                '/sitemap.xml',
                '/gsitemap.xml',
                '/sitemap/sitemap.xml'
            ],
            'Joomla': [
                '/index.php?option=com_xmap&view=xml',
                '/component/xmap/?view=xml',
                '/sitemap.xml'
            ],
            'Shopify': [
                '/sitemap.xml',
                '/sitemap_products_1.xml',
                '/sitemap_pages_1.xml',
                '/sitemap_collections_1.xml',
                '/sitemap_blogs_1.xml'
            ],
            'Magento': [
                '/sitemap.xml',
                '/pub/sitemap.xml',
                '/media/sitemap/sitemap.xml'
            ],
            'PrestaShop': [
                '/sitemap.xml',
                '/1_index_sitemap.xml',
                '/2_index_sitemap.xml'
            ]
        }
        
        for cms, patterns in cms_patterns.items():
            for pattern in patterns:
                url = self.base_url + pattern
                exists, _ = self.check_url(url, 5)
                if exists:
                    self.found_sitemaps.add(url)
                    print(f"  Found ({cms}): {url}")
    
    def check_subdomain_sitemaps(self):
        """Check common subdomains for sitemaps"""
        print(f"\nChecking subdomains...")
        
        subdomains = ['www', 'blog', 'shop', 'store', 'news', 'support', 'help']
        base_domain = self.domain.replace('www.', '')
        
        for subdomain in subdomains:
            if subdomain not in self.domain:
                subdomain_url = f"https://{subdomain}.{base_domain}/sitemap.xml"
                exists, _ = self.check_url(subdomain_url, 5)
                if exists:
                    self.found_sitemaps.add(subdomain_url)
                    print(f"  Found on subdomain: {subdomain_url}")
    
    def parse_sitemap_index(self, sitemap_url, content):
        """Parse sitemap index files for nested sitemaps"""
        print(f"\nParsing sitemap index: {sitemap_url}")
        
        soup = BeautifulSoup(content, 'xml')
        for loc in soup.find_all('loc'):
            nested_url = loc.text.strip()
            if nested_url and nested_url not in self.found_sitemaps:
                exists, nested_content = self.check_url(nested_url, 5)
                if exists:
                    self.found_sitemaps.add(nested_url)
                    print(f"  Found nested sitemap: {nested_url}")
                    
                    # Check if this is also an index
                    if nested_content and '<sitemapindex' in nested_content.lower():
                        self.parse_sitemap_index(nested_url, nested_content)
    
    def run(self):
        """Run all checks"""
        print(f"Searching for sitemaps on {self.domain}...")
        print("=" * 60)
        
        # Run all checks
        self.check_robots_txt()
        self.check_common_patterns()
        self.check_homepage_for_sitemap_links()
        self.check_well_known_locations()
        self.check_cms_specific_locations()
        self.check_subdomain_sitemaps()
        
        # Parse any sitemap indexes found
        print(f"\nChecking for nested sitemaps in indexes...")
        sitemap_indexes = list(self.found_sitemaps)
        for sitemap_url in sitemap_indexes:
            exists, content = self.check_url(sitemap_url)
            if exists and content and '<sitemapindex' in content.lower():
                self.parse_sitemap_index(sitemap_url, content)
        
        # Display results
        print("\n" + "=" * 60)
        print(f"SUMMARY - Found {len(self.found_sitemaps)} sitemap(s):")
        print("=" * 60)
        
        if self.found_sitemaps:
            for sitemap in sorted(self.found_sitemaps):
                print(f"✓ {sitemap}")
        else:
            print("❌ No sitemaps found")
            print("\nPossible reasons:")
            print("- The site might not have a sitemap")
            print("- Sitemaps might be behind authentication")
            print("- Non-standard sitemap location")
            print("- Dynamic sitemap generation")
        
        return self.found_sitemaps

# Run the script
if __name__ == "__main__":
    finder = SitemapFinder("easyapply.co")
    sitemaps = finder.run()
    
    # Save results to file
    with open('easyapply_sitemaps.txt', 'w') as f:
        f.write(f"Sitemap Discovery Results for easyapply.co\n")
        f.write(f"Generated on: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 60 + "\n\n")
        
        if sitemaps:
            f.write(f"Found {len(sitemaps)} sitemap(s):\n\n")
            for sitemap in sorted(sitemaps):
                f.write(f"{sitemap}\n")
        else:
            f.write("No sitemaps found\n")