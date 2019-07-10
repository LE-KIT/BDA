#!/usr/bin/env python
# coding: utf-8

# In[1]:


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
import zipfile


# In[2]:


def contains(word, lst):
    result = False
    for l in lst:
        if l.lower() in word.lower(): 
            result = True
            break
    return result


# In[3]:


#Weather stations
WS = ['15000', '00164', '00183', '00232', '00282', '00403', '00430', '00433', '00691', '00722', '00880', '00891', '01048', '01078', '01270', '05516', '01346', '01358', '01420', '01468', '01504', '05856', '01612', '01639', '01684', '01757', '01832', '05871', '01975', '02014', '02115', '02261', '02290', '02483', '02559', '02564', '02667', '02712', '02812', '02932', '00102', '02961', '03015', '03032', '03126', '05906', '03196', '03231', '01262', '01766', '03552', '03660', '03668', '03730', '03761', '07341', '03987', '04104', '04177', '04271', '04336', '04466', '04625', '04887', '04911', '04931', '05100', '00954', '01228', '05371', '05397', '05705', '05792']


# In[4]:


#Data 
sources = ['https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/',
           'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/']


# In[5]:


#Directories
downloadDir = '/Users/ozumerzifon/Desktop/test/zip/'
fileDir = '/Users/ozumerzifon/Desktop/test/txt/'


# In[6]:


options = Options()
options.add_experimental_option("prefs", {
  "download.binary_location": r"/usr/local/bin/chromedriver", 
  "download.default_directory": r"/Users/ozumerzifon/Desktop/test/zip/",
  "download.prompt_for_download": False,
  "download.directory_upgrade": True,
  "safebrowsing.enabled": True
})


# In[7]:


driver = webdriver.Chrome(chrome_options = options)


# In[8]:


counter_zip = 0
counter_txt = 0


# In[9]:


for s in sources:
    #Open source in browser
    driver.get(s)
    print('Starting download from {}'.format(s))
    #Find all links
    for a in driver.find_elements_by_xpath('.//a'):
        if contains(a.get_attribute('href'), WS):
            #Download file
            driver.get(a.get_attribute('href'))
            data = a.get_attribute('href').split('/')[-1]
            #Wait until download is finished
            while not os.path.exists(downloadDir+data):
                time.sleep(1)
            counter_zip += 1
            print('Download of {} finished.'.format(data))
            #Unzip the file    
            try:
                zip_ref = zipfile.ZipFile(downloadDir+data, 'r')
                for f in zip_ref.namelist():
                    if f[:17] == 'produkt_klima_tag':
                        zip_ref.extract(f, fileDir)
                        while not os.path.exists(fileDir+f):
                            time.sleep(1)
                        #os.rename(downloadDir+necessaryFile, fileDir+necessaryFile)
                        counter_txt += 1
                        print('{} extracted.'.format(f))
                zip_ref.close()
            except:
                print("Can't unzip {}".format(data))
            #Delete files
            os.remove(downloadDir+data) 


# In[10]:


print('ZIP files (downloaded): {}\nTXT files (downloaded): {}\nTXT files (in directory): {}'.format(counter_zip, counter_txt, len([x for x in os.listdir(fileDir) if x.endswith('.txt')])))


# In[11]:


driver.quit()


# In[ ]:




