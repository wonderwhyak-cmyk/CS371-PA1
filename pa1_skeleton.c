/*
# Copyright 2025 University of Kentucky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0
*/

/*
Please specify the group members here

# Student #1: Awashyek Kunwar
# Student #2: N/A
# Student #3: N/A

*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <pthread.h>

#include <errno.h>
#include <fcntl.h>

#define MAX_EVENTS 64
#define MESSAGE_SIZE 16
#define DEFAULT_CLIENT_THREADS 4

char *server_ip = "127.0.0.1";
int server_port = 12345;
int num_client_threads = DEFAULT_CLIENT_THREADS;
int num_requests = 1000000;

/*
 * This structure is used to store per-thread data in the client
 */
typedef struct {
    int epoll_fd;        /* File descriptor for the epoll instance, used for monitoring events on the socket. */
    int socket_fd;       /* File descriptor for the client socket connected to the server. */
    long long total_rtt; /* Accumulated Round-Trip Time (RTT) for all messages sent and received (in microseconds). */
    long total_messages; /* Total number of messages sent and received. */
    float request_rate;  /* Computed request rate (requests per second) based on RTT and total messages. */
} client_thread_data_t;

/* ---------- Helper functions (small + focused) ---------- */

static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
    return 0;
}

static long long time_diff_us(const struct timeval *start, const struct timeval *end) {
    long long s = (long long)start->tv_sec * 1000000LL + (long long)start->tv_usec;
    long long e = (long long)end->tv_sec * 1000000LL + (long long)end->tv_usec;
    return e - s;
}

static int send_all(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, 0);
        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            /* Busy-wait is not ideal, but for PA1 it’s acceptable; server/client are local/high-speed. */
            continue;
        }
        return -1;
    }
    return 0;
}

static int recv_exact(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    size_t recvd = 0;

    while (recvd < len) {
        ssize_t n = recv(fd, p + recvd, len - recvd, 0);
        if (n > 0) {
            recvd += (size_t)n;
            continue;
        }
        if (n == 0) return -1; /* peer closed */
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            /* caller should only call recv_exact after epoll says readable */
            continue;
        }
        return -1;
    }
    return 0;
}

/*
 * This function runs in a separate client thread to handle communication with the server
 */
void *client_thread_func(void *arg) {
    client_thread_data_t *data = (client_thread_data_t *)arg;
    struct epoll_event event, events[MAX_EVENTS];
    char send_buf[MESSAGE_SIZE] = "ABCDEFGHIJKMLNOP"; /* Send 16-Bytes message every time */
    char recv_buf[MESSAGE_SIZE];
    struct timeval start, end;

    // Hint 1: register the "connected" client_thread's socket in the its epoll instance
    // Hint 2: use gettimeofday() and "struct timeval start, end" to record timestamp, which can be used to calculated RTT.

    /* TODO:
     * It sends messages to the server, waits for a response using epoll,
     * and measures the round-trip time (RTT) of this request-response.
     */
    /* --- TODO (client per-thread loop using epoll + RTT timing) --- */
    memset(&event, 0, sizeof(event));
    event.events = EPOLLIN;
    event.data.fd = data->socket_fd;

    if (epoll_ctl(data->epoll_fd, EPOLL_CTL_ADD, data->socket_fd, &event) < 0) {
        die("client epoll_ctl(ADD)");
    }

    data->total_rtt = 0;
    data->total_messages = 0;
    data->request_rate = 0.0f;

    for (int i = 0; i < num_requests; i++) {
        if (gettimeofday(&start, NULL) < 0) die("gettimeofday(start)");

        if (send_all(data->socket_fd, send_buf, MESSAGE_SIZE) < 0) {
            die("client send_all");
        }

        /* Wait for server echo using epoll */
        while (1) {
            int nfds = epoll_wait(data->epoll_fd, events, MAX_EVENTS, -1);
            if (nfds < 0) {
                if (errno == EINTR) continue;
                die("client epoll_wait");
            }

            int got_readable = 0;
            for (int e = 0; e < nfds; e++) {
                if (events[e].data.fd == data->socket_fd && (events[e].events & EPOLLIN)) {
                    got_readable = 1;
                    break;
                }
                if (events[e].events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                    die("client socket error/hangup");
                }
            }

            if (got_readable) break;
        }

        if (recv_exact(data->socket_fd, recv_buf, MESSAGE_SIZE) < 0) {
            die("client recv_exact");
        }

        if (gettimeofday(&end, NULL) < 0) die("gettimeofday(end)");

        data->total_rtt += time_diff_us(&start, &end);
        data->total_messages += 1;
    }

    /* TODO:
     * The function exits after sending and receiving a predefined number of messages (num_requests).
     * It calculates the request rate based on total messages and RTT
     */
    /* --- TODO (request rate) --- */
    if (data->total_rtt > 0) {
        double total_seconds = (double)data->total_rtt / 1000000.0;
        data->request_rate = (float)((double)data->total_messages / total_seconds);
    } else {
        data->request_rate = 0.0f;
    }

    return NULL;
}

/*
 * This function orchestrates multiple client threads to send requests to a server,
 * collect performance data of each threads, and compute aggregated metrics of all threads.
 */
void run_client() {
    pthread_t threads[num_client_threads];
    client_thread_data_t thread_data[num_client_threads];
    struct sockaddr_in server_addr;

    /* TODO:
     * Create sockets and epoll instances for client threads
     * and connect these sockets of client threads to the server
     */
    /* --- TODO (create per-thread socket + epoll + connect) --- */
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < num_client_threads; i++) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) die("client socket");

        /* Connect first (simple + reliable). */
        if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            die("client connect");
        }

        /* Non-blocking helps avoid rare stalls; epoll still used for readiness. */
        if (set_nonblocking(sockfd) < 0) die("client set_nonblocking");

        int epfd = epoll_create1(0);
        if (epfd < 0) die("client epoll_create1");

        thread_data[i].socket_fd = sockfd;
        thread_data[i].epoll_fd = epfd;
        thread_data[i].total_rtt = 0;
        thread_data[i].total_messages = 0;
        thread_data[i].request_rate = 0.0f;
    }

    // Hint: use thread_data to save the created socket and epoll instance for each thread
    // You will pass the thread_data to pthread_create() as below
    for (int i = 0; i < num_client_threads; i++) {
        pthread_create(&threads[i], NULL, client_thread_func, &thread_data[i]);
    }

    /* TODO:
     * Wait for client threads to complete and aggregate metrics of all client threads
     */
    /* --- TODO (join + aggregate + cleanup) --- */
    long long total_rtt = 0;
    long total_messages = 0;
    float total_request_rate = 0.0f;

    for (int i = 0; i < num_client_threads; i++) {
        pthread_join(threads[i], NULL);

        total_rtt += thread_data[i].total_rtt;
        total_messages += thread_data[i].total_messages;
        total_request_rate += thread_data[i].request_rate;

        close(thread_data[i].socket_fd);
        close(thread_data[i].epoll_fd);
    }

    if (total_messages > 0) {
        printf("Average RTT: %lld us\n", total_rtt / total_messages);
    } else {
        printf("Average RTT: 0 us\n");
    }
    printf("Total Request Rate: %f messages/s\n", total_request_rate);
}

void run_server() {

    /* TODO:
     * Server creates listening socket and epoll instance.
     * Server registers the listening socket to epoll
     */
    /* --- TODO (server setup) --- */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) die("server socket");

    int opt = 1;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        die("server setsockopt(SO_REUSEADDR)");
    }

    if (set_nonblocking(listen_fd) < 0) die("server set_nonblocking(listen)");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)server_port);
    if (inet_pton(AF_INET, server_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        exit(EXIT_FAILURE);
    }

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) die("server bind");
    if (listen(listen_fd, 128) < 0) die("server listen");

    int epfd = epoll_create1(0);
    if (epfd < 0) die("server epoll_create1");

    struct epoll_event ev;
    memset(&ev, 0, sizeof(ev));
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;

    if (epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) die("server epoll_ctl(ADD listen)");

    /* Server's run-to-completion event loop */
    while (1) {
        /* TODO:
         * Server uses epoll to handle connection establishment with clients
         * or receive the message from clients and echo the message back
         */
        /* --- TODO (epoll accept + echo loop) --- */
        struct epoll_event events[MAX_EVENTS];
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            die("server epoll_wait");
        }

        for (int i = 0; i < nfds; i++) {
            int fd = events[i].data.fd;

            if (fd == listen_fd) {
                /* Accept as many as possible (non-blocking listen socket) */
                while (1) {
                    struct sockaddr_in caddr;
                    socklen_t clen = sizeof(caddr);
                    int cfd = accept(listen_fd, (struct sockaddr *)&caddr, &clen);
                    if (cfd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        die("server accept");
                    }

                    if (set_nonblocking(cfd) < 0) die("server set_nonblocking(client)");

                    struct epoll_event cev;
                    memset(&cev, 0, sizeof(cev));
                    cev.events = EPOLLIN | EPOLLRDHUP;
                    cev.data.fd = cfd;

                    if (epoll_ctl(epfd, EPOLL_CTL_ADD, cfd, &cev) < 0) {
                        die("server epoll_ctl(ADD client)");
                    }
                }
            } else {
                /* Client socket readable or closed */
                if (events[i].events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                    close(fd);
                    continue;
                }

                if (events[i].events & EPOLLIN) {
                    char buf[MESSAGE_SIZE];
                    /* Read exactly MESSAGE_SIZE so client stays in sync */
                    if (recv_exact(fd, buf, MESSAGE_SIZE) < 0) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                        close(fd);
                        continue;
                    }
                    if (send_all(fd, buf, MESSAGE_SIZE) < 0) {
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, NULL);
                        close(fd);
                        continue;
                    }
                }
            }
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc > 1 && strcmp(argv[1], "server") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);

        run_server();
    } else if (argc > 1 && strcmp(argv[1], "client") == 0) {
        if (argc > 2) server_ip = argv[2];
        if (argc > 3) server_port = atoi(argv[3]);
        if (argc > 4) num_client_threads = atoi(argv[4]);
        if (argc > 5) num_requests = atoi(argv[5]);

        run_client();
    } else {
        printf("Usage: %s <server|client> [server_ip server_port num_client_threads num_requests]\n", argv[0]);
    }

    return 0;
}
