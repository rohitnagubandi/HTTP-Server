#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <err.h>
#include <errno.h>

#include <sys/stat.h>
#include <sys/file.h>

#include "asgn4_helper_funcs.h"
#include "connection.h"
#include "debug.h"
#include "request.h"
#include "queue.h"
#include "response.h"
queue_t *queue = NULL;
char *TEMP_FILENAME = "mnb.txt";
void handle_connection(void);
void handle_get(conn_t *);
void handle_put(conn_t *);
void handle_unsupported(conn_t *);

char *get_rid(conn_t *conn) {
    char *id = conn_get_header(conn, "Request-Id");
    if (id == NULL) {
        id = "0";
    }
    return id;
}

void audit_log(char *name, char *uri, char *id, int code) {
    fprintf(stderr, "%s,/%s,%d,%s\n", name, uri, code, id);
}

void usage(FILE *stream, char *exec) {
    fprintf(stream, "usage: %s [-t threads] <port>\n", exec);
}

void acquire_exclusive(int fd) {
    int rc = flock(fd, LOCK_EX);
    assert(rc == 0);
    //debug("acquire_exclusive on %d", fd);
}

void acquire_shared(int fd) {
    int rc = flock(fd, LOCK_SH);
    assert(rc == 0);
    //debug("acquire_shared on %d", fd);
}

int acquire_templock(void) {
    int fd = open(TEMP_FILENAME, O_WRONLY);
    //debug("opened %d", fd);
    acquire_exclusive(fd);
    return fd;
}

void release(int fd) {
    //debug("release");
    flock(fd, LOCK_UN);
}

int main(int argc, char **argv) {
    int opt = 0;
    int threads = 4;
    pthread_t *threadids;
    int fd_temp = creat(TEMP_FILENAME, 0600);
    if (argc < 2) {
        warnx("wrong arguments: %s port_num", argv[0]);
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    while ((opt = getopt(argc, argv, "t:h")) != -1) {
        switch (opt) {
        case 't':
            threads = strtol(optarg, NULL, 10);
            if (threads <= 0) {
                errx(EXIT_FAILURE, "bad number of threads");
            }
            break;
        case 'h': usage(stdout, argv[0]); return EXIT_SUCCESS;
        default: usage(stdout, argv[0]); return EXIT_SUCCESS;
        }
    }
    if (optind >= argc) {
        warnx("wrong arguments: %s port_num", argv[0]);
        usage(stdout, argv[0]);
        return EXIT_FAILURE;
    }

    char *endptr = NULL;
    size_t port = (size_t) strtoull(argv[optind], &endptr, 10);
    if (endptr && *endptr != '\0') {
        warnx("invalid port number: %s", argv[1]);
        return EXIT_FAILURE;
    }

    signal(SIGPIPE, SIG_IGN);
    Listener_Socket sock;

    if (listener_init(&sock, port) < 0) {
        warnx("Can't open listener sock: %s", argv[0]);
        return EXIT_FAILURE;
    }

    threadids = malloc(sizeof(pthread_t) * threads);
    queue = queue_new(threads);

    for (int i = 0; i < threads; ++i) {
        int rc = pthread_create(threadids + 1, NULL, (void *(*) (void *) ) handle_connection, NULL);
        if (rc != 0) {
            warnx("Cannot create %d pthreads", threads);
            return EXIT_FAILURE;
        }
    }

    while (1) {
        uintptr_t connfd = listener_accept(&sock);
        queue_push(queue, (void *) connfd);
    }

    queue_delete(&queue);
    close(fd_temp);
    remove(TEMP_FILENAME);
    return EXIT_SUCCESS;
}

void handle_connection(void) {
    while (1) {
        uintptr_t connfd = 0;
        conn_t *conn = NULL;
        queue_pop(queue, (void **) &connfd);
        conn = conn_new(connfd);
        const Response_t *res = conn_parse(conn);

        if (res != NULL) {
            conn_send_response(conn, res);
        } else {
            //debug("%s", conn_str(conn));
            const Request_t *req = conn_get_request(conn);

            if (req == &REQUEST_GET) {
                handle_get(conn);
            } else if (req == &REQUEST_PUT) {
                handle_put(conn);
            } else {
                handle_unsupported(conn);
            }
        }
        conn_delete(&conn);
        close(connfd);
    }
}

void handle_get(conn_t *conn) {
    // TODO: Implement GET
    char *uri = conn_get_uri(conn);
    int lock = acquire_templock();
    int fd = open(uri, O_RDONLY);

    if (fd < 0) {
        char *id = get_rid(conn);
        if (errno == EACCES) {
            conn_send_response(conn, &RESPONSE_FORBIDDEN);
            audit_log("GET", uri, id, 403);
        } else if (errno == ENOENT) {
            conn_send_response(conn, &RESPONSE_NOT_FOUND);
            audit_log("GET", uri, id, 404);
        }
        release(lock);
        close(lock);

    } else {
        acquire_shared(fd);
        release(lock);
        close(lock);

        char *id = get_rid(conn);
        struct stat path_stat;
        fstat(fd, &path_stat);

        if (S_ISDIR(path_stat.st_mode)) {
            conn_send_response(conn, &RESPONSE_FORBIDDEN);
            audit_log("GET", uri, id, 403);
        }

        else {
            uint64_t size = (uint64_t) path_stat.st_size;
            conn_send_file(conn, fd, size);
            audit_log("GET", uri, id, 200);
        }
        release(fd);
        close(fd);
    }
    return;
}

/*
    struct stat path_stat;
    if (stat(uri, &path_stat) == 0) {
    	if (S_ISDIR(path_stat.st_mode)) {
    		conn_send_response(conn, &RESPONSE_FORBIDDEN);     
    		audit_log("GET",uri,id,403);
        }
    }

    int fd = open(uri, O_RDWR, 0666);
    if (fd==-1){
    	if (errno == EACCES){
    	    conn_send_response(conn, &RESPONSE_FORBIDDEN);
    	    audit_log("GET",uri,id,403);
    	}
    	else if (errno == ENOENT){
    	   	conn_send_response(conn, &RESPONSE_NOT_FOUND);
    	   	audit_log("GET",uri,id,404);
    	}
    }else{
		struct stat st;
		stat(uri,&st);
		uint64_t size = (uint64_t) st.st_size;
		conn_send_file(conn,fd, size);
		audit_log("GET",uri,id,200);
    }
    close(fd);
*/

void handle_put(conn_t *conn) {

    char *id = get_rid(conn);
    char *uri = conn_get_uri(conn);
    int lock = acquire_templock();
    int acc;
    if (access(uri, F_OK) == 0) {
        acc = 1;
    }

    int fd = open(uri, O_CREAT | O_WRONLY, 0600);

    struct stat path_stat;
    fstat(fd, &path_stat);

    if (S_ISDIR(path_stat.st_mode)) {
        conn_send_response(conn, &RESPONSE_FORBIDDEN);
        audit_log("GET", uri, id, 403);
        release(lock);
        close(lock);
        return;
    }

    if (fd < 0) {
        conn_send_response(conn, &RESPONSE_FORBIDDEN);
        audit_log("PUT", uri, id, 403);
        release(lock);
        close(lock);
    } else {
        acquire_exclusive(fd);
        release(lock);
        close(lock);
        int rc = ftruncate(fd, 0);
        assert(rc == 0);
        if (acc != 1) {
            conn_recv_file(conn, fd);
            conn_send_response(conn, &RESPONSE_CREATED);
            audit_log("PUT", uri, id, 201);
        } else {
            conn_recv_file(conn, fd);
            conn_send_response(conn, &RESPONSE_OK);
            audit_log("PUT", uri, id, 200);
        }
        release(fd);
        close(fd);
    }
    return;
}
/*
    struct stat path_stat;
    if (stat(uri, &path_stat) == 0) {
    	if (S_ISDIR(path_stat.st_mode)) {
    		conn_send_response(conn, &RESPONSE_FORBIDDEN);
			audit_log("PUT",uri,id,403);    		        
        }
    }
    debug("PUT %s", uri);
    int acc;
    if (access(uri, F_OK) == 0) {
    	acc = 1;
    }
    int fd = creat(uri,0666);
    if (fd==-1){
    	if (errno==EEXIST){
    		conn_recv_file(conn,fd);
    		conn_send_response(conn, &RESPONSE_OK);
    		audit_log("PUT",uri,id,200);
    	}
    	else if (errno == EACCES){
    		conn_send_response(conn, &RESPONSE_FORBIDDEN);
    		audit_log("PUT",uri,id,403);
    	}
    }else if (acc != 1){
   		conn_recv_file(conn,fd);
    	conn_send_response(conn, &RESPONSE_CREATED);   	
    	audit_log("PUT",uri,id,201);	
    }else{
   		conn_recv_file(conn,fd);
    	conn_send_response(conn, &RESPONSE_OK);
    	audit_log("PUT",uri,id,200);  		    
    }    
    close(fd);
    */

void handle_unsupported(conn_t *conn) {
    //debug("Unsupported request");
    char *id = get_rid(conn);
    char *uri = conn_get_uri(conn);
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
    audit_log("PUT", uri, id, 501);
    return;
}
