#coding:utf-8
import requests
from bs4 import BeautifulSoup as bs
import pymysql
from multiprocessing import Process
import time


class DataBase():
    
    def __init__(self):
        self.db = pymysql.connect('ip', 'id', 'passwd', 'database')#连接mysql，缺省4个参数
        self.cursor = self.db.cursor()
        
    def close(self):
        self.db.close()
        self.cursor.close()
    
    def create(self):
        #建立table，包括七个字段-爬到的ip，port，代理位置，类型，验证时间，还有首次入库分数和连接成功次数
        sql = "CREATE TABLE IF NOT EXISTS `66_proxy`(`ip` VARCHAR(15) NOT NULL,`port` VARCHAR(10) NOT NULL,`location` VARCHAR(20) NOT NULL,`type` VARCHAR(10) NOT NULL,`time` VARCHAR(30) NOT NULL,`num` TINYINT NOT NULL, `success` TINYINT, PRIMARY KEY(`ip`))CHARSET=utf8;"
        self.cursor.execute(sql) 
    
    def find(self, *args):
        a,b,c = args
        sql = 'select {} from 66_proxy where {}="{}"'.format(a,b,c)
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        if result == ():
            result = False
        return result
    
    def update(self,*args):
        a,b,c,d = args
        sql = 'update 66_proxy set {}={} where {}="{}"'.format(a,b,c,d)
        self.cursor.execute(sql)
        self.db.commit()
        
    def insert(self,save_list,num):
        ip, port,location, leixing, time = save_list
        values = (ip, port,location, leixing, time, num)
        sql = "INSERT INTO 66_proxy (ip, port,location, type, time, num) values(%s,%s,%s,%s,%s,%s)"
        self.cursor.execute(sql, values)
        self.db.commit()

    def delete(self, data):
        sql = "delete from 66_proxy where ip=%s"
        self.cursor.execute(sql, data)
        self.db.commit()        
                    
            
def get_page(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
    }
    r = requests.get(url, headers=headers)
    r.encoding=('gb2312')
    soup = bs(r.text)
    return soup

def page_info(html):
    #分析要获取的页面
    page = html.find('ul', class_="textlarge22").find_all("a")
    info = {}
    for p in page:
        info [p.text]= p.get('href')
    return info#返回字典

def get_proxy(dir,db):
	furl = dir.pop('全国代理ip')
	for num,(key,value) in enumerate(dir.items()):
		print('正在获取 %s ...' %key)
		url = furl+value
		proxys = get_page(url).find('div', id="main").find_all('td')[5:]
		temp = []
		for p in proxys:
			temp.append(p.text)                
			if len(temp)==5: 
				if not db.find('ip','ip',temp[0]):
					db.insert(temp,10)     #新获取的代理存入数据库设置测试次数10
					print("%s,已经保存到数据库" %temp[0])
				else:
					print('%s，已经存在' %temp[0])
				temp=[]  
		print('%s,保存成功' %key)
		print('*'*30)

def start():
    #爬取并仍入数据库
    main_page = "http://www.66ip.cn/"
    html = get_page(main_page)
    info = page_info(html)
    db = DataBase()
    db.create()
    get_proxy(info, db)
    db.close() 


def text(proxy):#测试代理是否可用
    url = 'http://icanhazip.com/'
    proxies = {
        "http": "http://%s:%s" %proxy,
        'https': "https://%s:%s" %proxy
    }
    try:
        r = requests.get(url,proxies=proxies,timeout=1).text#服务器返回一个ip+\n
        if r==proxy[0]+'\n':
            return True
    except Exception as e:
        return False
            
def can_use(proxy, score):
    print('正在测试代理%s...' %proxy[0])
    while score>0:#测试新获取的是否可用
        if text(proxy) ==True:
            return True
        score -= 1
    return False
            
def f1(proxy, score):
    print('正在给%s评分...' %proxy[0])
    count = 0
    while score>0:#返回连接成功的次数
        if text(proxy) ==True:
            count += 1
        score -= 1
    print('%s分数为:%s' %(proxy[0],str(count)))
    return count        
    
def run(num):#检测可用性和稳定性
    db = DataBase()
    tl = db.find('ip,port,num','num',num)
    for i in tl:
        ip = i[0]
        port = i[1]
        num_1 = i[2] 
        if num ==10:#测试新代理，10次无法连接删除
            bol = can_use((ip,port),num_1)
            if not bol:
                db.delete(ip)
                print("%s无法使用已经从数据库删除" %ip)
            else:
                db.update("num",100,"ip",ip)#把新代理，标识为100
                print("%s通过测试" %ip)  
        elif num ==100:#
            count = f1((ip,port),num_1)
            if count < 90:
            	db.delete(ip)
            	print("%s分数低于90已经删除" %ip)#筛选稳定的代理
            else:
            	db.update("success", count, 'ip', ip)  
            	print("%s已经保存分数" %ip)          
    db.close()


def main():
    while True:
        localtime = time.asctime( time.localtime(time.time()) )
        print("本地时间为 :", localtime)
        t1 = Process(target=start)
        t2 = Process(target=run, args=(10,))
        t3 = Process(target=run, args=(100,))
        t1.start()
        t2.start()
        t3.start()
        t1.join()            
        t2.join()            
        t3.join()
        localtime = time.asctime( time.localtime(time.time()) )
        print("本地时间为 :", localtime)
        time.sleep(1800)
        
if __name__ == "__main__":
    main()
    