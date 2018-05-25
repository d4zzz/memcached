// memcached.cpp : 定义控制台应用程序的入口点。
//

#include"stdafx.h"
#include<winsock2.h>
#pragma comment(lib,"ws2_32.lib")
#include <stdlib.h>
#include<regex>
#include<vector>
#include<map>
#include<iostream>
using namespace std;

#define BUF_SIZE 1024
#define LISTEN_PORT 11211

/*
定义内存结点
*/
struct mem{
	string key;
	string flags; 
	string exptime;
	string len;
	string noreply;
	string data;
};

vector<mem> dict;
vector<mem>::iterator it;

/*
传递给通信子线程的参数
*/
struct param
{
	SOCKET sock;
	sockaddr_in sockaddr;
};

WSADATA wsd;
SOCKET sServer;
SOCKET sClient;
char buf[BUF_SIZE];
int retVal;

int initServer();//初始化服务器，做好连接准备
DWORD WINAPI AnswerThread(LPVOID lparam);//线程相应函数，一个线程处理一个客户端的请求
void parseReq(string Req,SOCKET client);//对请求进行解析并返回相应值
bool isExist(string key);//是否存在该键值对应的结点
void parseStc(string in,vector<string> &out);//处理客户端原始请求语句
mem findValueByKey(string key);//根据Key找对应Value

bool isValidKey(string in);//判断参数字符串有效性
bool isValidInt(string in);
bool isValidUINT(string in);
bool isValidNoreply(string in);
bool isValidKey(string in);
int _tmain(int argc, _TCHAR* argv[]) {
	
	if (initServer()) {
		printf("initialize failed!\n");
		return 0;
	}

	//接收来自客户端的请求
	printf("memcached server start... \n");
	sockaddr_in addrClient;
	int addrClientLen = sizeof(addrClient);
	
	while (true) {
		sClient = accept(sServer, (struct sockaddr FAR*)&addrClient, &addrClientLen);
		if (sClient == INVALID_SOCKET) {
			int err = WSAGetLastError();//如果没有接收到连接请求就继续循环等待
			if (err == WSAEWOULDBLOCK) {
				Sleep(100);
				continue;
			}
		}
		param * args = new param;//将socket和sockaddr传给线程函数
		args->sock = sClient;
		args->sockaddr = addrClient;
		DWORD dwThreadId;
		CreateThread(NULL, 0, AnswerThread, args, 0, &dwThreadId);//线程函数处理请求
	}
	closesocket(sServer);
	closesocket(sClient);
	WSACleanup();

	system("pause");
	return 0;
}

int initServer() {
	//初始化winsock2.2
	if (WSAStartup(MAKEWORD(2, 2), &wsd) != 0) {
		printf("WSAStartup initialize failed!");
		return 1;
	}
	//创建用于监听的 Socket
	sServer = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (sServer == INVALID_SOCKET) {
		printf("socket error!");
		WSACleanup();
		return -1;

	}
	int iMode = 1;
	retVal = ioctlsocket(sServer, FIONBIO, (u_long FAR*)&iMode);
	if (SOCKET_ERROR == retVal) {
		printf("ioctlsocket fail!");
		WSACleanup();
		return -1;
	}

	//指定服务器SOcket地址
	SOCKADDR_IN addrServ;
	addrServ.sin_family = AF_INET;
	addrServ.sin_port = htons(LISTEN_PORT);
	addrServ.sin_addr.S_un.S_addr = htonl(INADDR_ANY);


	//绑定Sockets Server到本地地址
	retVal = bind(sServer, (const struct sockaddr*)&addrServ, sizeof(SOCKADDR_IN));
	if (retVal == SOCKET_ERROR) {
		printf("bind error!");
		closesocket(sServer);
		WSACleanup();
		return -1;
	}
	//在Sockets Serverz 上进行监听
	retVal = listen(sServer, 10);
	if (retVal == SOCKET_ERROR) {
		printf("listen error!");
		closesocket(sServer);
		WSACleanup();
		return -1;
	}
	return 0;
}

DWORD WINAPI AnswerThread(LPVOID lparam) {
	int retVal;
	char buf[BUF_SIZE];
	param* args = (param*)lparam;
	SOCKET sClient = args->sock;

	while (true) {

		ZeroMemory(buf, BUF_SIZE);
		retVal = recv(sClient, buf, BUF_SIZE, 0);
		if (SOCKET_ERROR == retVal) {
			int err = WSAGetLastError();
			if (err == WSAEWOULDBLOCK) {
				Sleep(500);
				continue;
			}
			else if (err == WSAENETDOWN || err == WSAETIMEDOUT) {
				printf("recv failed!");
				closesocket(sClient);
				WSACleanup();
				return -1;
			}
		}
		SYSTEMTIME st;
		GetLocalTime(&st);
		char sDateTime[30];
		sockaddr_in addrClient=args->sockaddr;
		/*sprintf_s(sDateTime, "%4d-%2d-%2d %2d:%2d:%2d", st.wYear, st.wMonth, st.wDay, st.wHour, st.wMinute, st.wSecond);
		printf("%s,Recv From Client[%s:%d]:%s\n", sDateTime, inet_ntoa(addrClient.sin_addr), addrClient.sin_port, buf);
		printf(">");*/
		if (strcmp(buf, "quit\r\n") == 0) {
			//retVal = send(sClient, "bye", strlen("bye"), 0);
			break;
		}
		else
		{
			parseReq((string)buf, sClient);
		}


	}
	closesocket(sClient);
}

void parseReq(string Req,SOCKET client) {
	char msg[BUF_SIZE];
	ZeroMemory(msg, BUF_SIZE);
	vector<string> args;
	parseStc(Req, args);
	int argsLen = args.size();
	mem dt;
	if (regex_match(Req, regex("^set.*\r\n$"))) {//处理set请求
		if (argsLen < 5 || argsLen>6) {
			sprintf_s(msg, "ERROR\r\n");
		}
		else if(argsLen == 5){//有四个输入，即无noreply
			dt.key = args[1];
			dt.flags = args[2];
			dt.exptime = args[3];
			dt.len = args[4];	
			if (isValidUINT(dt.flags) && isValidKey(dt.key) && isValidInt(dt.exptime) && isValidUINT(dt.len)) {
				ZeroMemory(buf, BUF_SIZE);
				dt.data = "";
				int dataLen = atoi(dt.len.c_str())+2;
				while (dataLen>0) {
					retVal = recv(client, buf, BUF_SIZE, 0);
					if (SOCKET_ERROR == retVal) {
						int err = WSAGetLastError();
						if (err == WSAEWOULDBLOCK) {
							Sleep(500);
							continue;
						}
						else if (err == WSAENETDOWN || err == WSAETIMEDOUT) {
							printf("recv failed!");
							closesocket(sClient);
							WSACleanup();

						}
					}
					dt.data += buf;
					dataLen -= strlen(buf);
				}
				if ((dt.data.size()-2) == atoi(dt.len.c_str())) {
					if (atoi(dt.exptime.c_str()) >= 0) {//对生存时间为负的不保存
						dict.push_back(dt);
					}
					sprintf_s(msg, "STORED\r\n");
				}
				else if ((dt.data.size() - 2) > atoi(dt.len.c_str())) {
					sprintf_s(msg, "CLIENT_ERROR bad data chunk\r\nERROR\r\n");
				}
				
			}
			else {
				sprintf_s(msg, "CLIENT_ERROR bad command line format\r\n");
			}
		}
		else {//有五个输入参数，即多最后一位noreply
			dt.key = args[1];
			dt.flags = args[2];
			dt.exptime = args[3];
			dt.len = args[4];
			dt.noreply = args[5];
			if (isValidUINT(dt.flags) && isValidKey(dt.key) && isValidInt(dt.exptime) && isValidUINT(dt.len)) {
				ZeroMemory(buf, BUF_SIZE);
				ZeroMemory(buf, BUF_SIZE);
				dt.data = "";
				int dataLen = atoi(dt.len.c_str());
				while (dataLen>0) {
					retVal = recv(client, buf, BUF_SIZE, 0);
					if (SOCKET_ERROR == retVal) {
						int err = WSAGetLastError();
						if (err == WSAEWOULDBLOCK) {
							Sleep(500);
							continue;
						}
						else if (err == WSAENETDOWN || err == WSAETIMEDOUT) {
							printf("recv failed!");
							closesocket(client);
							WSACleanup();

						}
					}
					dt.data += buf;
					dataLen -= strlen(buf);
				}
				if ((dt.data.size() - 2) == atoi(dt.len.c_str())) {
					if (atoi(dt.exptime.c_str()) >= 0) {//对生存时间为负的不保存
						dict.push_back(dt);
					}
					if (isValidNoreply(dt.noreply)) {
						sprintf_s(msg, "");
					}
					else {
						sprintf_s(msg, "STORED\r\n");
					}
				}
				else if ((dt.data.size() - 2) > atoi(dt.len.c_str())) {
					if (isValidNoreply(dt.noreply)) {
						sprintf_s(msg, "ERROR\r\n");
					}
					else {
						sprintf_s(msg, "CLIENT_ERROR bad data chunk\r\nERROR\r\n");
					}
				}
				
				
			}
			else {
				sprintf_s(msg, "CLIENT_ERROR bad command line format\r\n");
			}
		}
		while (true) {
			retVal = send(client, msg, strlen(msg), 0);
			if (SOCKET_ERROR == retVal) {
				int err = WSAGetLastError();
				if (err == WSAEWOULDBLOCK)
				{
					Sleep(500);
					continue;
				}
				else {
					printf("send faied!\n");
					closesocket(client);
					WSACleanup();
				}
			}
			break;
		}
	}
	else if (regex_match(Req, regex("^get.*\r\n$"))) {//处理get请求
		string mg;
		for (int i = 1; i < argsLen; i++) {
			if (isExist(args[i])) {
				mem res= findValueByKey(args[i]);
				mg = mg + "VALUE "+res.key+" "+res.flags+" "+res.len+"\r\n"+res.data;
			}
		}
		sprintf_s(msg, "%sEND\r\n",mg.c_str());
		while (true) {
			retVal = send(client, msg, strlen(msg), 0);
			if (SOCKET_ERROR == retVal) {
				int err = WSAGetLastError();
				if (err == WSAEWOULDBLOCK)
				{
					Sleep(500);
					continue;
				}
				else {
					printf("send faied!\n");
					closesocket(client);
					WSACleanup();
				}
			}
			break;
		}
	}
	else if (regex_match(Req, regex("^delete.*\r\n$"))) {//处理delete请求
		string mg;
		if (argsLen > 4) {//请求参数个数大于4的返回ERROR
			sprintf_s(msg, "ERROR\r\n");
		}
		else if (argsLen > 2) {//请求参数个数大于2的返回CLIENT_ERROR
			sprintf_s(msg, "CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]\r\n");
		}
		else {
				if (isExist(args[1])) {//查询是否有该键值
					if (dict.begin()->key == args[1]) {//判断是否是第一个
						dict.erase(dict.begin());
					}
					else {
						for (it = dict.begin(); it != dict.end(); it++) {//有则删除
							if (it->key == args[1]) {
								dict.erase(it);
							}
						}
					}
					sprintf_s(msg, "DELETED\r\n");
				}
				else {//无则返回NOT_FOUND
					sprintf_s(msg, "NOT_FOUND\r\n");
				}
		}	
		while (true) {//向客户端发送返回信息
			retVal = send(client, msg, strlen(msg), 0);
			if (SOCKET_ERROR == retVal) {
				int err = WSAGetLastError();
				if (err == WSAEWOULDBLOCK)
				{
					Sleep(500);
					continue;
				}
				else {
					printf("send faied!\n");
					closesocket(client);
					WSACleanup();
				}
			}
			break;
		}
	}
	else {//对于其他请求返回ERROR\r\n
		//ZeroMemory(msg, BUF_SIZE);
		sprintf_s(msg, "ERROR\r\n");
		while (true) {
			retVal = send(client, msg, strlen(msg), 0);
			if (SOCKET_ERROR == retVal) {
				int err = WSAGetLastError();
				if (err == WSAEWOULDBLOCK)
				{
					Sleep(500);
					continue;
				}
				else {
					printf("send faied!\n");
					closesocket(client);
					WSACleanup();
				}
			}
			break;
		}
	}
}

bool isExist(string key) {//根据键值判断是否存在该条目
	for (size_t i = 0; i < dict.size(); i++)
	{
		if (dict[i].key == key) {
			return true;
		}
	}
	return false;
}

void parseStc(string in, vector<string> &out) {//将请求以空格为界分成参数组，返回字符串容器
	string s=in;
	smatch m;
	regex e("(\\b[a-z0-9]+\\b)");   

	while (regex_search(s, m, e)) {

		out.push_back(m.str());
		s = m.suffix().str();
	}

}
mem findValueByKey(string key)//根据Key来找Value
{
	for (size_t i = 0; i < dict.size(); i++) 
	{
		if (dict[i].key == key) {
			return dict[i];
		}
	}
}
bool isValidKey(string in)//判断是否是有效的Key，字符串个数应在250以下
{
	if (in.size() > 0 && in.size() < 251) {
		return true;
	}
	return false;
}
bool isValidInt(string in)//判断是否是含正负数的整数
{
	if (regex_match(in, regex("[-]?[0-9]+"))) {
		return true;
	}
	return false;
}
bool isValidUINT(string in)//判断是否是无符号正整数
{
	if (regex_match(in, regex("[0-9]+"))) {
		return true;
	}
	return false;
}
bool isValidNoreply(string in)//判断是否是“noreply”
{
	if (regex_match(in, regex("^noreply$"))) {
		return true;
	}
	return false;
}