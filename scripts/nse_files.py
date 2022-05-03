import requests
import zipfile
import io
from datetime import datetime
import datetime as dtime
import csv
import urllib.request
from time import time

def download_nse_files(days = 0):

    today_date = datetime.today().date() - dtime.timedelta(days=days)
    if today_date.isoweekday() < 6:
        year = today_date.year
        month = today_date.month
        date = today_date.day

        # To download previous day data
        # date = datetime.today().day - 1

        m = '%02d' % month
        d = '%02d' % date
        month_MMM = {'01': 'JAN', '02': 'FEB', '03': 'MAR', '04': 'APR', '05': 'MAY', '06': 'JUN', '07': 'JUL', '08': 'AUG',
                     '09': 'SEP', '10': 'OCT', '11': 'NOV', '12': 'DEC'}

        url = "https://www1.nseindia.com/content/historical/DERIVATIVES/{0}/{1}/fo{2}{1}{0}bhav.csv.zip".format(year,
                                                                                                                month_MMM[m], d)

        url_eq = "https://www1.nseindia.com/content/historical/EQUITIES/{0}/{1}/cm{2}{1}{0}bhav.csv.zip".format(year,
                                                                                                                month_MMM[m], d)

        url_OI = "https://www1.nseindia.com/content/nsccl/fao_participant_oi_{2}{1}{0}.csv".format(year, m, d)

        # url_MTO = "https://www1.nseindia.com/archives/equities/mto/MTO_{2}{1}{0}.DAT".format(year,m,d)

        res = None

        hdr = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*,q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-IN,en;q=0.9,en-GB;q=0.8,en-US;q=0.7,hi;q=0.6',
            'Connection': 'keep-alive', 'Host': 'www1.nseindia.com',
            'Cache-Control': 'max-age=0',
            'Host': 'www1.nseindia.com',
            'Referer': 'https://www1.nseindia.com/products/content/derivatives/equities/fo.htm',
            }
        cookie_dict = {
            'bm_sv': 'E2109FAE3F0EA09C38163BBF24DD9A7E~t53LAJFVQDcB/+q14T3amyom/sJ5dm1gV7z2R0E3DKg6WiKBpLgF0t1Mv32gad4CqvL3DIswsfAKTAHD16vNlona86iCn3267hHmZU/O7DrKPY73XE6C4p5geps7yRwXxoUOlsqqPtbPsWsxE7cyDxr6R+RFqYMoDc9XuhS7e18='}
        session = requests.session()
        for cookie in cookie_dict:
            session.cookies.set(cookie, cookie_dict[cookie])

        #Get derivative data
        response = session.get(url, headers=hdr)
        if response.status_code != 200:
            print('response.status_code ', response.status_code)

        #Get equity data
        response1 = session.get(url_eq, headers=hdr)
        if response1.status_code != 200:
            print('response1.status_code ', response.status_code)

        #Get f&o derivative data
        file_name = "none";
        try:
            zipT = zipfile.ZipFile(io.BytesIO(response.content))
            zipT.extractall(f"..\\master_download_dump\\nse_f&o\\{year}\\")

            file_name = zipT.filelist[0].filename
        except zipfile.BadZipFile:
            print('Error: Zip file is corrupted')
        except zipfile.LargeZipFile:
            print('Error: File size if too large')

        #Get Equity trade data
        file_name = "none";
        try:
            zipT = zipfile.ZipFile(io.BytesIO(response1.content))
            zipT.extractall(f"..\\master_download_dump\\nse_equity\\{year}\\")

            file_name = zipT.filelist[0].filename
        except zipfile.BadZipFile:
            print(f'Error: Zip file {file_name} is corrupted')
        except zipfile.LargeZipFile:
            print('Error: File size if too large')

        #Get F&O participation data
        with requests.Session() as s:
            download_OI = s.get(url_OI)
            decoded_content_OI = download_OI.content.decode('utf-8')
            try:
                if '404 Not Found' in decoded_content_OI:
                    raise FileNotFoundError(
                        'Open Interest file name - ' + "fao_participant_oi_" + str(d) + str(m) + str(year) + ".csv")
                OI = csv.reader(decoded_content_OI.splitlines(), delimiter=',')
                OI_List = list(OI)
                csv_file = open(f"..\\master_download_dump\\participant_wise_open_interest\\{year}\\fao_participant_oi_{d}{m}{year}.csv",
                    'w+', newline='')

                with csv_file:
                    write = csv.writer(csv_file)
                    write.writerows(OI_List)
            except Exception as e:
                print("Exception : " + repr(e))

        #Get delivery volume and  Indices data for the day
        def url_response(url):
            path, url = url
            r = requests.get(url, stream=True)
            try:
                if r.status_code != 200:
                    raise FileNotFoundError('Unable to download file at', path)
                with open(path, 'wb') as f:
                    for ch in r:
                        f.write(ch)
            except Exception as e:
                 print("Exception : " + repr(e))

        urls = [
            (f"..\\master_download_dump\\delivery_volumes\\{year}\\{year}{m}{d}.csv",
             f"https://www1.nseindia.com/archives/equities/mto/MTO_{d}{m}{year}.DAT")
        ]

        start = time()
        for x in urls:
            url_response(x)

        urls = [
            (f"..\\master_download_dump\\indices\\{year}\\{year}{m}{d}.csv",
             f"https://www1.nseindia.com/content/indices/ind_close_all_{d}{m}{year}.csv")
        ]

        start = time()
        for x in urls:
            url_response(x)