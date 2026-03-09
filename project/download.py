import requests
import xml.etree.ElementTree as ET
import urllib.parse
from pathlib import Path

BUCKET = "cycling.data.tfl.gov.uk"
S3_HOST = "s3-eu-west-1.amazonaws.com"
PREFIX = "ActiveTravelCountsProgramme/"

def fetch_all_links(prefix=""):
    links = []
    token = None

    
    Path("project/downloads").mkdir(parents=True, exist_ok=True)

    while True:
        url = f"https://{S3_HOST}/{BUCKET}/?list-type=2&delimiter=/&prefix={urllib.parse.quote(prefix)}"
        if token:
            url += f"&continuation-token={urllib.parse.quote(token)}"

        res = requests.get(url)
        root = ET.fromstring(res.text)
        ns = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

        for i, content in enumerate(root.findall('s3:Contents', ns)):
            key = content.find('s3:Key', ns).text
            if key in [prefix, "index.html"]:
                continue
            encoded = "/".join(urllib.parse.quote(p) for p in key.split("/"))
            link = f"https://{BUCKET}/{encoded}"
            links.append(link)

            if link.endswith('.csv') and "Q" in link:
                filename = urllib.parse.unquote(link.split('/')[-1])
                filepath = f'project/downloads/{filename}'
                
                print(f"Downloading: {filename}")
            
                with requests.get(link, stream=True) as r:
                    r.raise_for_status()
                    with open(filepath, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024*1024):
                            f.write(chunk)

        is_truncated = root.find('s3:IsTruncated', ns)
        if is_truncated is not None and is_truncated.text == "true":
            token = root.find('s3:NextContinuationToken', ns).text
        else:
            break

    return links

if __name__ == "__main__":
    links = fetch_all_links(PREFIX)
    print(f"Found {len(links)} files")