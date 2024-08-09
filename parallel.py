import time
import fire
from utilmy import pd_read_file
import pandas as pd
import chromedriver_autoinstaller
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

def pd_parallel_apply(df, myfunc, colout="llm_json", npool=4, ptype="process", **kwargs):
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")

    def worker(row, **kwargs):
        return myfunc(row, **kwargs)
    
    results = []

    if ptype == "process":
        from concurrent.futures import ProcessPoolExecutor as mp
        with mp(max_workers=npool) as executor:
            futures = [executor.submit(myfunc, row, **kwargs) for _, row in df.iterrows()]

    else:
        from concurrent.futures import ThreadPoolExecutor as mp
        with mp(max_workers=npool) as executor:
            futures = [executor.submit(worker, row, **kwargs) for _, row in df.iterrows()]

    for future in futures:
        results.append(future.result())

    df[colout] = results
    return df 

def fetch_url(url):
    chromedriver_autoinstaller.install()

    chrome_options = Options()
    chrome_options.add_argument("--headless")  

    service = Service()
    browser = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        browser.get(url)
        WebDriverWait(browser, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        current_url = browser.current_url
    finally:
        browser.quit() 

    return current_url

def url_getfinal_url(file: str):
    df = pd_read_file(file)
    final_df = pd_parallel_apply(df, fetch_url)
    print (final_df)
    return final_df

if __name__ == '__main__':
    fire.Fire(url_getfinal_url)