import json
import logging
import os
import httplib
import ConfigParser
import errno
import socket
from socket import error as socket_error
import smtplib
from smtplib import SMTPException
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from jinja2 import Environment
import psycopg2
import telnetlib
import datetime

config = ConfigParser.RawConfigParser()
config.read('./config.ini')

#Log file
log = "./spider.log"

environment = config.get('ENVIRONMENT', 'ENVIRONMENT')
front = config.get('HOST', 'FRONT_HOST')
frontInternet = config.get('HOST', 'FRONTINTERNET_HOST')
ckanPort = config.get('PORT', 'CKAN_PORT')
corePort = config.get('PORT', 'CORE_PORT')
bbddPort = config.get('PORT', 'BBDD_PORT')
nodePort = config.get('PORT', 'NODE_PORT')
vdataPort = config.get('PORT', 'VISUALDATA_PORT')
elasticPort = config.get('PORT', 'ELASTIC_PORT')
protocolHttp = config.get('PROTOCOL', 'HTTP_PROTOCOL')
protocolHttps = config.get('PROTOCOL', 'HTTPS_PROTOCOL')

coreViews = []

fullTest = True

responseERR = {
    "errors": []
}

files = {
    "files": [
        {
            "fileName": "./jsonFiles/static_URLs.json",
            "path": None,
            "detailPath": None,
            "type": "URLs est\160ticas",
            "active": 1
        },
        {
            "fileName": "./jsonFiles/datasets.json",
            "path": "/api/action/package_list",
            "detailPath": "/api/action/package_show?id=",
            "type": str(frontInternet) + "/datos/catalogo/dataset/",
            "active": 1
        },
        {
            "fileName": "./jsonFiles/topics.json",
            "path": "/api/action/group_list",
            "detailPath": "/api/action/group_show?id=",
            "type": str(frontInternet) + "/datos/catalogo/temas/",
            "active": 1
        },
        {
            "fileName": "./jsonFiles/organizations.json",
            "path": "/api/action/organization_list",
            "detailPath": "/api/action/organization_show?id=",
            "type": str(frontInternet) + "/datos/catalogo/publicadores/",
            "active": 1
        },
        {
            "fileName": "./jsonFiles/coreViews.json",
            "path": "/GA_OD_Core/views",
            "detailPath": "/GA_OD_Core/show_columns?view_id=",
            "type": str(frontInternet) + "/GA_OD_Core/preview?view_id=",
            "active": 1
        }
    ]
}

def getProperty(field):
    section = field.split("_")[1]
    return str(config.get(section, field))

def getJSONData(file):
    if "GA_OD_Core" in file["type"]:
        url = buildURL(frontInternet, "", file["path"])
        conn = getConnectionType("https", frontInternet)
    else:
        url = buildURL(front, ckanPort, file["path"])
        conn = getConnectionType("http", front)
        setConnectionPort(conn, ckanPort)
    res = makeRequest("GET", conn, file["path"])
    saveStatus(url, res.status, file["type"])
    writeFiles(file, res)
    conn.close()

def saveStatus(url, res, service):
    logFile = open(log, 'a+')
    try:
        if res != 200:
            saveErr(logFile, url, service)
        else:
            if fullTest:
                logFile.write("[" + str(datetime.datetime.now()) + "]\t" + "FULL\t" + service + "\t" + url + '\t-> OK' + '\r\n')
            else:
                logFile.write("[" + str(datetime.datetime.now()) + "]\t" + "BASIC\t" + service + "\t" + url + '\t-> OK' + '\r\n')
    except AttributeError as err:
            saveErr(logFile, url, service)
    logFile.close()

def saveErr(logFile, url, service):
    if fullTest:
        logFile.write("[" + str(datetime.datetime.now()) + "]\t" + "FULL\t" + service + "\t" + url + '\t-> NOK' + '\r\n')
    else:
        logFile.write("[" + str(datetime.datetime.now()) + "]\t" + "BASIC\t" + service + "\t" + url + '\t-> NOK' + '\r\n')
    responseERR["errors"].append({
        "service": service,
        "url": url,
    })

def writeFiles(file, res):
    f = open(file["fileName"], 'w')
    if "GA_OD_Core" in file["type"]:
        coreViews = res.read()
        f.write(coreViews)
    else:
        jsonData = json.loads(res.read())
        f.write(json.dumps(jsonData["result"]))
    f.close()

def resetFiles(file):
    open(file["fileName"], 'w').close()
    
def readGeneralConnectionsFile(file, jsonFiles):
    f = open(file["fileName"], 'r')
    contentFile = json.loads(f.read())
    for content in contentFile:
        if content["host"] != "None" and content["active"] == 1:
            getProperties(content)
            if "GA_OD_Core" in file["type"]:
                testConnection(protocolHttps, frontInternet, "", file["path"], file["type"])
            else:
                testConnection(content["protocol"], content["host"], content["port"], content["path"], content["service"])
        else:
            if content["active"] == 0:
                setActive(content, jsonFiles)
        f.close()

def getProperties(content):
    content["protocol"] = getProperty(str(content["protocol"])[1:])
    if content["host"].startswith('%'):
        content["host"] = getProperty(str(content["host"])[1:])
    if str(content["port"]).startswith('%'):
        content["port"] = getProperty(str(content["port"])[1:])

def setActive(content, jsonFiles):
    if content["service"] == "CKAN_RESOURCES":
        jsonFiles[1]["active"] = 0
        jsonFiles[2]["active"] = 0
        jsonFiles[3]["active"] = 0
    elif content["service"] == "GA_OD_Core_VIEWS":
        jsonFiles[4]["active"] = 0

def testConnection(protocol, host, port, path, service):
    conn = None
    url = buildURL(host, port, path)
    try:
        if port == str(bbddPort) or port == str(nodePort) or port == str(vdataPort) or port == str(elasticPort):
            tn = telnetlib.Telnet(host, port)
            if tn != None:
                res = 200
        else:
            conn = getConnectionType(protocol, host)
            setConnectionPort(conn, port)
            res = makeRequest("GET", conn, path)
            if "GA_OD_Core" in service:
                if checkViewMsg(res):
                    res = res.status
                else:
                    res = "View error"
            else:
                res = res.status
    except socket_error as err:
        res = err
    except httplib.BadStatusLine as badStatus:
        res = badStatus
    print 'Comprobando ' + url
    saveStatus(url, res, service)
    if conn:
        conn.close()

def checkViewMsg(res):
    isOk = False
    viewMsg = res.read()
    if "Something went wrong" not in viewMsg and "must be a number" not in viewMsg and "not exist" not in viewMsg:
        isOk = True
    return isOk

def makeRequest(method, conn, path):
    conn.request(method, path)
    return conn.getresponse()

def getConnectionType(protocol, host):
    if protocol == "http":
        return httplib.HTTPConnection(host)
    else:
        return httplib.HTTPSConnection(host)

def setConnectionPort(conn, port):
    if port != "":
        conn.port = port

def buildURL(host, port, path):
    if port != "":
        url = url = host + ':' + str(port) + path
    else:
        url = host + path
    return url

def readData(file):
    if file["active"] == 1:
        f = open(file["fileName"], 'r')
        contentFile = json.loads(f.read())
        if "GA_OD_Core" in file["type"]:
            for content in contentFile:
                testConnection(protocolHttps, frontInternet, "", file["detailPath"]+str(content[0]), file["type"]+str(content[0])+"&_pageSize=1&_page=1")
        else:
            for content in contentFile:
                testConnection(protocolHttp, front, ckanPort, file["detailPath"]+content, file["type"]+content)
        f.close()

def main():
    checkFullTest()
    jsonFiles = json.dumps(files)
    jsonFiles = json.loads(jsonFiles)
    for file in jsonFiles['files']:
        if file['path'] is not None:
            if file["active"] == 1:
                resetFiles(file)
                getJSONData(file)
                readData(file)
        else:
            readGeneralConnectionsFile(file, jsonFiles['files'])
    if len(responseERR["errors"]) > 0 and not checkEmailRevision():
        sendReportByEmail()
        changeEmailRevision()

def changeEmailRevision():
    f = open('./emailRevision.json', 'r+')
    content = json.loads(f.read())
    if content["emailRevision"] == 0:
        content["emailRevision"] = 1
    else:
        content["emailRevision"] = 0
    f.seek(0)
    f.write(json.dumps(content))
    f.truncate()
    f.close()

def checkEmailRevision():
    toCheck = True
    f = open('./emailRevision.json', 'r')
    content = json.loads(f.read())
    if content["emailRevision"] == 0:
        toCheck = False
    f.close()
    return toCheck
    
def sendReportByEmail():
    print "Enviando informe de testeo de las conexiones por email..."
    fromaddr = config.get('EMAIL', 'SENDER')
    toaddr = config.get('EMAIL', 'RECEIVER')
    msg = buildMessage(fromaddr, toaddr)
    sendEmail(fromaddr, toaddr, msg)

def buildMessage(fromaddr, toaddr):
    msg = MIMEMultipart()
    setMessageHeaders(fromaddr, toaddr, msg)
    contentFile = []
    if len(responseERR["errors"]) > 0:
        title = "INFORME DE FALLOS EN LAS CONEXIONES: " + "\r\n"
        contentFile = responseERR["errors"]
    renderTemplate(title, msg, contentFile)
    
    text = msg.as_string()
    return text

def renderTemplate(title, msg, contentFile):
    template = open('./resources/mailTemplate.html', 'r')
    msg.attach(MIMEText(
        Environment().from_string(template.read()).render(
            title=msg['Subject'],
            header=title,
            contentFile=contentFile
        ), "html"
    ))
    template.close()

def setMessageHeaders(fromaddr, toaddr, msg):
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Informe de conexiones en entorno " + environment

def sendEmail(fromaddr, toaddr, msg):
    server = connectToSMTPServer(fromaddr)
    server.sendmail(fromaddr, toaddr, msg)
    server.quit()

def connectToSMTPServer(fromaddr):
    server = smtplib.SMTP("smtp.aragon.es", 587)
    server.starttls()
    server.login(fromaddr, config.get('EMAIL', 'PASS'))
    return server

def checkFullTest():
    global fullTest
    f = open('./jsonFiles/static_URLs.json')
    contentFile = json.loads(f.read())
    for content in contentFile:
        if content["active"] == 0 and content["host"] == "None":
            fullTest = False
    f.close()

if __name__ == '__main__':
    main()
