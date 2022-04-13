import sys
import re
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import time
from tqdm import tqdm
import calendar
import os
import pandas as pd
from TikTokApi import TikTokApi
from lxml import html

DIR_PATH = os.getcwd()
source_folder = os.path.dirname(str(os.path.abspath(sys.argv[0])))


def create_folder(out_folder, folder_name):
    out_path = os.path.join(out_folder, folder_name)
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    return out_path


def error_log(error,source_url=""):
    log_path = DIR_PATH+"/log/error.txt"
    with open(log_path, 'a') as outfile:
        outfile.write(source_url+"-"+error)
        outfile.write('\n')

# scroll to dynamically load the page
def scroll_page():
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def download_video(real_video_path, file_path):
    try:
        if not file_path:
            return None
        chunk_size = 1024
        r = requests.get(real_video_path, stream=True)
        total_size = int(r.headers['content-length'])
        with open(file_path, 'wb') as f:
            for data in tqdm(iterable=r.iter_content(chunk_size=chunk_size), total=total_size / chunk_size, unit='KB'):
                f.write(data)
        print("Download completed!")
        return True
    except:
        return None


def tiktok_download_function(video_url, downloaded_file_path):
    try:
        with TikTokApi() as api:
            print("Downloading video..")
            vd = api.video(
                url=video_url).bytes()

            with open(downloaded_file_path, 'wb') as output:
                output.write(vd)
                print("Downloaded..")
    except Exception as e:
        print("Error occurred while downloading..")
        error_log(error=str(e))


out_folder_path = create_folder(source_folder, "out")
log_folder_path = create_folder(source_folder, "log")

temp_folder_path = create_folder(out_folder_path, "temp")


def resume_log(file_path):
    log_path = DIR_PATH + "/log/resume_log.txt"
    with open(log_path, 'a') as outfile:
        outfile.write(file_path)
        outfile.write('\n')


def read_resume_log(file_path):
    ret = False
    log_path = DIR_PATH + "/log/resume_log.txt"
    if not os.path.isfile(log_path):
        return ret
    with open(log_path) as file_in:
        for i, line in enumerate(file_in):
            if str(line).strip() == file_path:
                ret = True
                break
    return ret


chromeOptions = webdriver.ChromeOptions()
chromeOptions.add_experimental_option("prefs", {
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True,
})

url = "https://www.tiktok.com/search?q=sri+lanka+dance"
driver = webdriver.Chrome(ChromeDriverManager().install(), options=chromeOptions)
driver.get(url)
last_height = driver.execute_script("return document.body.scrollHeight")
print("Loading..")
i = 0
out_arr = []
while True:

    scroll_page()
    time.sleep(5)

    # get links to this
    old_df = pd.DataFrame(out_arr)

    soup = BeautifulSoup(driver.page_source, "lxml")
    tags = ""
    data_containers = soup.find_all("div", {"class": "tiktok-1soki6-DivItemContainerForSearch eqfnwek9"})
    for data_container in data_containers:
        try:
            in_arr = []
            video_link = data_container.find("div", {"class": "tiktok-yz6ijl-DivWrapper e1t9ijiy1"}).find("a")['href']
            original_name = data_container.find("span", {"class": "tiktok-j2a19r-SpanText e7nizj40"}).text

            tag_list = data_container.find("div", {"class": "tiktok-1ejylhp-DivContainer e18aywvs0"}).find_all("a")

            for x, tag in enumerate(tag_list):
                if x == 0:
                    tags = str(tag.text)
                else:
                    tags = tags + "|" + str(tag.text)
            in_arr.append(video_link)
            in_arr.append(original_name)
            in_arr.append(tags)
            out_arr.append(in_arr)
        except:
            pass

    # video_links = driver.find_elements_by_xpath("//div[contains(@class, 'tiktok-yz6ijl-DivWrapper e1t9ijiy1')]/a")
    new_df = pd.DataFrame(out_arr)
    frames = [old_df, new_df]
    result_df = pd.concat(frames).drop_duplicates(keep=False).sort_index()
    result_df.to_csv(temp_folder_path + "/result_" + str(i) + ".csv", index=False)

    # video downloading process
    i = 0
    for index, row in result_df.iterrows():
        original_name = row[1]
        url = row[0]
        tags = row[2]
        if original_name:
            pattern = r'[^A-Za-z0-9]+'
            sample_str = re.sub(pattern, '', original_name)
            video_folder_path = create_folder(out_folder_path, str(sample_str))
            file_path = video_folder_path + "/" + str(sample_str) + ".mp4"
            csv_path = video_folder_path + "/" + str(sample_str) + ".csv"
        else:
            video_folder_path = create_folder(out_folder_path, str(i) + "_video")
            file_path = video_folder_path + "/" + str(i) + "_video" + ".mp4"
            csv_path = video_folder_path + "/" + str(i) + "_details" + ".csv"
        i = i + 1
        if read_resume_log(file_path):
            continue
        data = [[str(url), str(original_name), tags]]

        cols = ['Video link', 'Name', "tags"]
        df = pd.DataFrame(data, columns=cols)

        df.to_csv(csv_path, index=False)
        tiktok_download_function(url, file_path)
        resume_log(file_path)

    try:

        element = driver.find_element_by_xpath("//button[contains(text(),'Load more')]")
        driver.execute_script("arguments[0].click();", element)
        time.sleep(5)
    except:
        break
    if i > 3:
        break

    i = i + 1
