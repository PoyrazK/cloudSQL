/**
 * @file server.c
 * @brief Network server implementation
 *
 * @defgroup server Network Server
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>

#include "network/server.h"

/* Maximum backlog connections */
#define BACKLOG 10

/* Read buffer size */
#define READ_BUFFER_SIZE 8192

/* Write buffer size */
#define WRITE_BUFFER_SIZE 8192

/**
 * @brief Client connection structure
 */
typedef struct {
    int fd;
    struct sockaddr_in addr;
    char read_buffer[READ_BUFFER_SIZE];
    size_t read_offset;
    char write_buffer[WRITE_BUFFER_SIZE];
    size_t write_offset;
    bool connected;
} client_connection_t;

/**
 * @brief Create a new server instance
 */
server_t *server_create(config_t *config, catalog_t *catalog) {
    server_t *server;
    
    if (config == NULL) {
        return NULL;
    }
    
    server = ALLOC_ZERO(sizeof(server_t));
    if (server == NULL) {
        return NULL;
    }
    
    server->config = config;
    server->catalog = catalog;
    server->listen_fd = -1;
    server->running = false;
    server->stopping = false;
    
    /* Initialize callbacks */
    memset(&server->callbacks, 0, sizeof(server->callbacks));
    
    /* Initialize statistics */
    memset(&server->stats, 0, sizeof(server->stats));
    
    return server;
}

/**
 * @brief Destroy a server instance
 */
void server_destroy(server_t *server) {
    if (server == NULL) {
        return;
    }
    
    if (server->listen_fd >= 0) {
        close(server->listen_fd);
    }
    
    FREE(server);
}

/**
 * @brief Set server callbacks
 */
void server_set_callbacks(server_t *server, server_callbacks_t *callbacks) {
    if (server == NULL || callbacks == NULL) {
        return;
    }
    
    server->callbacks = *callbacks;
}

/**
 * @brief Create listening socket
 */
static int create_listening_socket(uint16_t port) {
    int fd;
    int opt = 1;
    struct sockaddr_in addr;
    
    /* Create socket */
    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        fprintf(stderr, "Cannot create socket: %s\n", strerror(errno));
        return -1;
    }
    
    /* Set socket options */
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        fprintf(stderr, "Cannot set SO_REUSEADDR: %s\n", strerror(errno));
        close(fd);
        return -1;
    }
    
    /* Bind to address */
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot bind to port %d: %s\n", port, strerror(errno));
        close(fd);
        return -1;
    }
    
    /* Listen for connections */
    if (listen(fd, BACKLOG) < 0) {
        fprintf(stderr, "Cannot listen: %s\n", strerror(errno));
        close(fd);
        return -1;
    }
    
    return fd;
}

/**
 * @brief Set socket to non-blocking mode
 */
static int set_nonblocking(int fd) {
    int flags;
    
    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return -1;
    }
    
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/**
 * @brief Handle client connection
 */
static int handle_client(server_t *server, int client_fd) {
    char buffer[READ_BUFFER_SIZE];
    ssize_t bytes_read;
    
    /* Read query from client */
    bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes_read <= 0) {
        /* Connection closed or error */
        if (bytes_read < 0) {
            fprintf(stderr, "Read error: %s\n", strerror(errno));
        }
        return -1;
    }
    
    server->stats.bytes_received += bytes_read;
    
    /* Null terminate the query */
    buffer[bytes_read] = '\0';
    
    /* Log the query */
    if (server->config->verbose) {
        printf("Received query: %s\n", buffer);
    }
    
    /* Process query (placeholder) */
    const char *response = "SELECT 1;\n";
    send(client_fd, response, strlen(response), 0);
    
    server->stats.bytes_sent += strlen(response);
    server->stats.queries_executed++;
    
    return 0;
}

/**
 * @brief Accept new connection
 */
static int accept_connection(server_t *server) {
    struct sockaddr_in addr;
    socklen_t addrlen = sizeof(addr);
    int client_fd;
    
    client_fd = accept(server->listen_fd, (struct sockaddr *)&addr, &addrlen);
    if (client_fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;  /* No pending connections */
        }
        fprintf(stderr, "Accept failed: %s\n", strerror(errno));
        return -1;
    }
    
    /* Disable Nagle's algorithm for lower latency */
    int flag = 1;
    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    
    /* Log connection */
    char *client_ip = inet_ntoa(addr.sin_addr);
    printf("New connection from %s:%d\n", client_ip, ntohs(addr.sin_port));
    
    server->stats.connections_accepted++;
    server->stats.connections_active++;
    
    return client_fd;
}

/**
 * @brief Start the server
 */
int server_start(server_t *server) {
    if (server == NULL) {
        return -1;
    }
    
    /* Create listening socket */
    server->listen_fd = create_listening_socket(server->config->port);
    if (server->listen_fd < 0) {
        return -1;
    }
    
    /* Set non-blocking */
    if (set_nonblocking(server->listen_fd) < 0) {
        close(server->listen_fd);
        server->listen_fd = -1;
        return -1;
    }
    
    server->running = true;
    
    printf("Server listening on port %d\n", server->config->port);
    
    return 0;
}

/**
 * @brief Stop the server
 */
int server_stop(server_t *server) {
    if (server == NULL) {
        return -1;
    }
    
    server->stopping = true;
    server->running = false;
    
    return 0;
}

/**
 * @brief Wait for server to stop
 */
void server_wait(server_t *server) {
    fd_set read_fds;
    int max_fd;
    int ret;
    
    if (server == NULL || server->listen_fd < 0) {
        return;
    }
    
    while (server->running && !server->stopping) {
        FD_ZERO(&read_fds);
        FD_SET(server->listen_fd, &read_fds);
        max_fd = server->listen_fd;
        
        /* Wait for activity */
        ret = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;  /* Interrupted by signal */
            }
            break;
        }
        
        if (ret == 0) {
            continue;  /* Timeout, shouldn't happen with NULL timeout */
        }
        
        /* Check for new connections */
        if (FD_ISSET(server->listen_fd, &read_fds)) {
            int client_fd = accept_connection(server);
            if (client_fd > 0) {
                handle_client(server, client_fd);
                close(client_fd);
                server->stats.connections_active--;
            }
        }
    }
    
    /* Close listening socket */
    if (server->listen_fd >= 0) {
        close(server->listen_fd);
        server->listen_fd = -1;
    }
}

/**
 * @brief Get server statistics
 */
server_stats_t server_get_stats(server_t *server) {
    server_stats_t stats = {0};
    
    if (server != NULL) {
        stats = server->stats;
    }
    
    return stats;
}

/**
 * @brief Get server status as string
 */
const char *server_get_status(server_t *server) {
    if (server == NULL) {
        return "NULL";
    }
    
    if (!server->running) {
        return "stopped";
    }
    
    if (server->stopping) {
        return "stopping";
    }
    
    return "running";
}

/** @} */ /* server */
