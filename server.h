/*
 * server.h
 *
 *  Created on: Nov 10, 2017
 *      Author: alex
 */

#ifndef SERVER_H_
#define SERVER_H_

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h>

using namespace std;

class Server {
private:
	int master_socket;
	int port;
	struct sockaddr_in address;
	vector<int>* client_socket;
	int max_sd = 0;
	fd_set readfds;
public:
	Server() {
		master_socket = -1;
		port = 8080;
		client_socket = new vector<int>();
	}
	Server(int p) {
		port = p;
		master_socket = -1;
		client_socket = new vector<int>();
	}
	virtual ~Server() {
		delete client_socket;
	}
	int get_master_socket() {
		return master_socket;
	}
	void error(const char *msg) {
		perror(msg);
		exit(1);
	}
	void initServer() {
		struct sockaddr_in serv_addr, cli_addr;
		// open socket
		master_socket = socket(AF_INET, SOCK_STREAM, 0);
		if (master_socket < 0)
			error("ERROR opening socket");

		bzero((char *) &serv_addr, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = INADDR_ANY;
		serv_addr.sin_port = htons(port);
		if (bind(master_socket, (struct sockaddr *) &serv_addr,
				sizeof(serv_addr)) < 0)
			error("ERROR on binding");
		listen(master_socket, 5);
	}
	bool is_valid_master(){
		return FD_ISSET(master_socket, &readfds);
	}
	bool is_valid_socket(int& sd){
		return FD_ISSET(sd, &readfds);
	}
	void closeServer() {
		if (master_socket != -1) {
			close(master_socket);
		}
	}
	void clear_set(){
		FD_ZERO(&readfds);
	}
	void check_valid_connection() {
		//add child sockets to set
		int sd;
		for (int i = 0; i < client_socket->size(); i++) {
			//socket descriptor
			sd = client_socket->at(i);
			//if valid socket descriptor then add to read list
			if (sd > 0)
				add_connection(sd);
			//highest file descriptor number, need it for the select function
			if (sd > max_sd)
				max_sd = sd;
		}
	}
	void add_connection(int sd){
		FD_SET(sd, &readfds);
	}
	int wait_connection(){
		return select(max_sd + 1 , &readfds , NULL , NULL , NULL);
	}
	int openConnection(char* buffer) {
		struct sockaddr_in cli_addr;
		socklen_t clilen;
		clilen = sizeof(cli_addr);
		int newsockfd = accept(master_socket, (struct sockaddr *) &cli_addr,
				&clilen);
		if (newsockfd < 0)
			error("ERROR on accept");
		bzero(buffer, 256);
		int n = read(newsockfd, buffer, 255);
		if (n < 0)
			error("ERROR reading from socket");
		client_socket->push_back(newsockfd);
		return newsockfd;
	}
	void update_connections(int& new_socket){
		client_socket->push_back(new_socket);
	}
	void closeConnection(int connid) {
		if (connid != -1) {
			close(connid);
			client_socket->erase(remove(client_socket->begin(), client_socket->end(), connid), client_socket->end());
		}
	}
	void sendMessage(int clid, const char* response) {
		int n = write(clid, response, strlen(response));
		if (n < 0)
			error("ERROR writing to socket");
	}
};

#endif /* SERVER_H_ */
