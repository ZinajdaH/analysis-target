static __attribute__ ((unused)) const char* state2String(int state){
    switch(state){
    case 0:
        return "ZOO_CLOSED_STATE";
    case CONNECTING_STATE_DEF:
        return "ZOO_CONNECTING_STATE";
    case SSL_CONNECTING_STATE_DEF:
        return "ZOO_SSL_CONNECTING_STATE";
    case ASSOCIATING_STATE_DEF:
        return "ZOO_ASSOCIATING_STATE";
    case CONNECTED_STATE_DEF:
        return "ZOO_CONNECTED_STATE";
    case READONLY_STATE_DEF:
        return "ZOO_READONLY_STATE";
    case EXPIRED_SESSION_STATE_DEF:
        return "ZOO_EXPIRED_SESSION_STATE";
    case AUTH_FAILED_STATE_DEF:
        return "ZOO_AUTH_FAILED_STATE";
    }
    return "INVALID_STATE";
}

const int ZOO_CREATED_EVENT = CREATED_EVENT_DEF;
const int ZOO_DELETED_EVENT = DELETED_EVENT_DEF;
const int ZOO_CHANGED_EVENT = CHANGED_EVENT_DEF;
const int ZOO_CHILD_EVENT = CHILD_EVENT_DEF;
const int ZOO_SESSION_EVENT = SESSION_EVENT_DEF;
const int ZOO_NOTWATCHING_EVENT = NOTWATCHING_EVENT_DEF;
static __attribute__ ((unused)) const char* watcherEvent2String(int ev){
    switch(ev){
    case 0:
        return "ZOO_ERROR_EVENT";
    case CREATED_EVENT_DEF:
        return "ZOO_CREATED_EVENT";
    case DELETED_EVENT_DEF:
        return "ZOO_DELETED_EVENT";
    case CHANGED_EVENT_DEF:
        return "ZOO_CHANGED_EVENT";
    case CHILD_EVENT_DEF:
        return "ZOO_CHILD_EVENT";
    case SESSION_EVENT_DEF:
        return "ZOO_SESSION_EVENT";
    case NOTWATCHING_EVENT_DEF:
        return "ZOO_NOTWATCHING_EVENT";
    }
    return "INVALID_EVENT";
}

const int ZOO_PERM_READ = 1 << 0;
const int ZOO_PERM_WRITE = 1 << 1;
const int ZOO_PERM_CREATE = 1 << 2;
const int ZOO_PERM_DELETE = 1 << 3;
const int ZOO_PERM_ADMIN = 1 << 4;
const int ZOO_PERM_ALL = 0x1f;
struct Id ZOO_ANYONE_ID_UNSAFE = {"world", "anyone"};
struct Id ZOO_AUTH_IDS = {"auth", ""};
static struct ACL _OPEN_ACL_UNSAFE_ACL[] = {{0x1f, {"world", "anyone"}}};
static struct ACL _READ_ACL_UNSAFE_ACL[] = {{0x01, {"world", "anyone"}}};
static struct ACL _CREATOR_ALL_ACL_ACL[] = {{0x1f, {"auth", ""}}};
struct ACL_vector ZOO_OPEN_ACL_UNSAFE = { 1, _OPEN_ACL_UNSAFE_ACL};
struct ACL_vector ZOO_READ_ACL_UNSAFE = { 1, _READ_ACL_UNSAFE_ACL};
struct ACL_vector ZOO_CREATOR_ALL_ACL = { 1, _CREATOR_ALL_ACL_ACL};

#define COMPLETION_WATCH -1
#define COMPLETION_VOID 0
#define COMPLETION_STAT 1
#define COMPLETION_DATA 2
#define COMPLETION_STRINGLIST 3
#define COMPLETION_STRINGLIST_STAT 4
#define COMPLETION_ACLLIST 5
#define COMPLETION_STRING 6
#define COMPLETION_MULTI 7
#define COMPLETION_STRING_STAT 8

typedef struct _auth_completion_list {
    void_completion_t completion;
    const char *auth_data;
    struct _auth_completion_list *next;
} auth_completion_list_t;

typedef struct completion {
    int type; /* one of COMPLETION_* values above */
    union {
        void_completion_t void_result;
        stat_completion_t stat_result;
        data_completion_t data_result;
        strings_completion_t strings_result;
        strings_stat_completion_t strings_stat_result;
        acl_completion_t acl_result;
        string_completion_t string_result;
        string_stat_completion_t string_stat_result;
        struct watcher_object_list *watcher_result;
    };
    completion_head_t clist; /* For multi-op */
} completion_t;

typedef struct _completion_list {
    int xid;
    completion_t c;
    const void *data;
    buffer_list_t *buffer;
    struct _completion_list *next;
    watcher_registration_t* watcher;
    watcher_deregistration_t* watcher_deregistration;
} completion_list_t;

const char*err2string(int err);
static inline int calculate_interval(const struct timeval *start,
        const struct timeval *end);
static int queue_session_event(zhandle_t *zh, int state);
static const char* format_endpoint_info(const struct sockaddr_storage* ep);

/* deserialize forward declarations */
static void deserialize_response(zhandle_t *zh, int type, int xid, int failed, int rc, completion_list_t *cptr, struct iarchive *ia);
static int deserialize_multi(zhandle_t *zh, int xid, completion_list_t *cptr, struct iarchive *ia);

/* completion routine forward declarations */
static int add_completion(zhandle_t *zh, int xid, int completion_type,
        const void *dc, const void *data, int add_to_front,
        watcher_registration_t* wo, completion_head_t *clist);
static int add_completion_deregistration(zhandle_t *zh, int xid,
        int completion_type, const void *dc, const void *data,
        int add_to_front, watcher_deregistration_t* wo,
        completion_head_t *clist);
static int do_add_completion(zhandle_t *zh, const void *dc, completion_list_t *c,
        int add_to_front);

static completion_list_t* create_completion_entry(zhandle_t *zh, int xid, int completion_type,
        const void *dc, const void *data, watcher_registration_t* wo,
        completion_head_t *clist);
static completion_list_t* create_completion_entry_deregistration(zhandle_t *zh,
        int xid, int completion_type, const void *dc, const void *data,
        watcher_deregistration_t* wo, completion_head_t *clist);
static completion_list_t* do_create_completion_entry(zhandle_t *zh,
        int xid, int completion_type, const void *dc, const void *data,
        watcher_registration_t* wo, completion_head_t *clist,
        watcher_deregistration_t* wdo);
static void destroy_completion_entry(completion_list_t* c);
static void queue_completion_nolock(completion_head_t *list, completion_list_t *c,
        int add_to_front);
static void queue_completion(completion_head_t *list, completion_list_t *c,
        int add_to_front);
static int handle_socket_error_msg(zhandle_t *zh, int line, int rc,
    const char* format,...);
static void cleanup_bufs(zhandle_t *zh,int callCompletion,int rc);

static int disable_conn_permute=0; // permute enabled by default
static struct sockaddr_storage *addr_rw_server = 0;

static void *SYNCHRONOUS_MARKER = (void*)&SYNCHRONOUS_MARKER;
static int isValidPath(const char* path, const int mode);
#ifdef HAVE_OPENSSL_H
static int init_ssl_for_handler(zhandle_t *zh);
static int init_ssl_for_socket(zsock_t *fd, zhandle_t *zh, int fail_on_error);
#endif

static int aremove_watches(
    zhandle_t *zh, const char *path, ZooWatcherType wtype,
    watcher_fn watcher, void *watcherCtx, int local,
    void_completion_t *completion, const void *data, int all);

#ifdef THREADED
static void process_sync_completion(zhandle_t *zh,
        completion_list_t *cptr,
        struct sync_completion *sc,
        struct iarchive *ia);

static int remove_watches(
    zhandle_t *zh, const char *path, ZooWatcherType wtype,
    watcher_fn watcher, void *watcherCtx, int local, int all);
#endif

#ifdef _WIN32
typedef SOCKET socket_t;
typedef int sendsize_t;
#define SEND_FLAGS  0
#else
#ifdef __APPLE__
#define SEND_FLAGS SO_NOSIGPIPE
#endif
#ifdef __linux__
#define SEND_FLAGS MSG_NOSIGNAL
#endif
#ifndef SEND_FLAGS
#define SEND_FLAGS 0
#endif
typedef int socket_t;
typedef ssize_t sendsize_t;
#endif

static void zookeeper_set_sock_nodelay(zhandle_t *, socket_t);
static void zookeeper_set_sock_noblock(zhandle_t *, socket_t);
static void zookeeper_set_sock_timeout(zhandle_t *, socket_t, int);
static socket_t zookeeper_connect(zhandle_t *, struct sockaddr_storage *, socket_t);

/*
 * return 1 if zh has a SASL client performing authentication, 0 otherwise.
 */
static int is_sasl_auth_in_progress(zhandle_t* zh)
{
#ifdef HAVE_CYRUS_SASL_H
    return zh->sasl_client && zh->sasl_client->state == ZOO_SASL_INTERMEDIATE;
#else /* !HAVE_CYRUS_SASL_H */
    return 0;
#endif /* HAVE_CYRUS_SASL_H */
}

#ifndef THREADED
/*
 * abort due to the use of a sync api in a singlethreaded environment
 */
static void abort_singlethreaded(zhandle_t *zh)
{
    LOG_ERROR(LOGCALLBACK(zh), "Sync completion used without threads");
    abort();
}
#endif  /* THREADED */

static ssize_t zookeeper_send(zsock_t *fd, const void* buf, size_t len)
{
#ifdef HAVE_OPENSSL_H
    if (fd->ssl_sock)
        return (ssize_t)SSL_write(fd->ssl_sock, buf, (int)len);
#endif
    return send(fd->sock, buf, len, SEND_FLAGS);
}

static ssize_t zookeeper_recv(zsock_t *fd, void *buf, size_t len, int flags)
{
#ifdef HAVE_OPENSSL_H
    if (fd->ssl_sock)
        return (ssize_t)SSL_read(fd->ssl_sock, buf, (int)len);
#endif
    return recv(fd->sock, buf, len, flags);
}

/**
 * Get the system time.
 *
 * If the monotonic clock is available, we use that.  The monotonic clock does
 * not change when the wall-clock time is adjusted by NTP or the system
 * administrator.  The monotonic clock returns a value which is monotonically
 * increasing.
 *
 * If POSIX monotonic clocks are not available, we fall back on the wall-clock.
 *
 * @param tv         (out param) The time.
 */
void get_system_time(struct timeval *tv)
{
  int ret;

#ifdef __MACH__ // OS X
  clock_serv_t cclock;
  mach_timespec_t mts;
  ret = host_get_clock_service(mach_host_self(), SYSTEM_CLOCK, &cclock);
  if (!ret) {
    ret += clock_get_time(cclock, &mts);
    ret += mach_port_deallocate(mach_task_self(), cclock);
    if (!ret) {
      tv->tv_sec = mts.tv_sec;
      tv->tv_usec = mts.tv_nsec / 1000;
    }
  }
  if (ret) {
    // Default to gettimeofday in case of failure.
    ret = gettimeofday(tv, NULL);
  }
#elif defined CLOCK_MONOTONIC_RAW
  // On Linux, CLOCK_MONOTONIC is affected by ntp slew but CLOCK_MONOTONIC_RAW
  // is not.  We want the non-slewed (constant rate) CLOCK_MONOTONIC_RAW if it
  // is available.
  struct timespec ts = { 0 };
  ret = clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  tv->tv_sec = ts.tv_sec;
  tv->tv_usec = ts.tv_nsec / 1000;
#elif _POSIX_MONOTONIC_CLOCK
  struct timespec ts = { 0 };
  ret = clock_gettime(CLOCK_MONOTONIC, &ts);
  tv->tv_sec = ts.tv_sec;
  tv->tv_usec = ts.tv_nsec / 1000;
#elif _WIN32
  LARGE_INTEGER counts, countsPerSecond, countsPerMicrosecond;
  if (QueryPerformanceFrequency(&countsPerSecond) &&
      QueryPerformanceCounter(&counts)) {
    countsPerMicrosecond.QuadPart = countsPerSecond.QuadPart / 1000000;
    tv->tv_sec = (long)(counts.QuadPart / countsPerSecond.QuadPart);
    tv->tv_usec = (long)((counts.QuadPart % countsPerSecond.QuadPart) /
        countsPerMicrosecond.QuadPart);
    ret = 0;
  } else {
    ret = gettimeofday(tv, NULL);
  }
#else
  ret = gettimeofday(tv, NULL);
#endif
  if (ret) {
    abort();
  }
}

const void *zoo_get_context(zhandle_t *zh)
{
    return zh->context;
}

void zoo_set_context(zhandle_t *zh, void *context)
{
    if (zh != NULL) {
        zh->context = context;
    }
}

int zoo_recv_timeout(zhandle_t *zh)
{
    return zh->recv_timeout;
}

/** these functions are thread unsafe, so make sure that
    zoo_lock_auth is called before you access them **/
static auth_info* get_last_auth(auth_list_head_t *auth_list) {
    auth_info *element;
    element = auth_list->auth;
    if (element == NULL) {
        return NULL;
    }
    while (element->next != NULL) {
        element = element->next;
    }
    return element;
}

static void free_auth_completion(auth_completion_list_t *a_list) {
    auth_completion_list_t *tmp, *ftmp;
    if (a_list == NULL) {
        return;
    }
    tmp = a_list->next;
    while (tmp != NULL) {
        ftmp = tmp;
        tmp = tmp->next;
        ftmp->completion = NULL;
        ftmp->auth_data = NULL;
        free(ftmp);
    }
    a_list->completion = NULL;
    a_list->auth_data = NULL;
    a_list->next = NULL;
    return;
}

static void add_auth_completion(auth_completion_list_t* a_list, void_completion_t* completion,
                                const char *data) {
    auth_completion_list_t *element;
    auth_completion_list_t *n_element;
    element = a_list;
    if (a_list->completion == NULL) {
        //this is the first element
        a_list->completion = *completion;
        a_list->next = NULL;
        a_list->auth_data = data;
        return;
    }
    while (element->next != NULL) {
        element = element->next;
    }
    n_element = (auth_completion_list_t*) malloc(sizeof(auth_completion_list_t));
    n_element->next = NULL;
    n_element->completion = *completion;
    n_element->auth_data = data;
    element->next = n_element;
    return;
}

static void get_auth_completions(auth_list_head_t *auth_list, auth_completion_list_t *a_list) {
    auth_info *element;
    element = auth_list->auth;
    if (element == NULL) {
        return;
    }
    while (element) {
        if (element->completion) {
            add_auth_completion(a_list, &element->completion, element->data);
        }
        element->completion = NULL;
        element = element->next;
    }
    return;
}

static void add_last_auth(auth_list_head_t *auth_list, auth_info *add_el) {
    auth_info  *element;
    element = auth_list->auth;
    if (element == NULL) {
        //first element in the list
        auth_list->auth = add_el;
        return;
    }
    while (element->next != NULL) {
        element = element->next;
    }
    element->next = add_el;
    return;
}

static void init_auth_info(auth_list_head_t *auth_list)
{
    auth_list->auth = NULL;
}

static void mark_active_auth(zhandle_t *zh) {
    auth_list_head_t auth_h = zh->auth_h;
    auth_info *element;
    if (auth_h.auth == NULL) {
        return;
    }
    element = auth_h.auth;
    while (element != NULL) {
        element->state = 1;
        element = element->next;
    }
}

static void free_auth_info(auth_list_head_t *auth_list)
{
    auth_info *auth = auth_list->auth;
    while (auth != NULL) {
        auth_info* old_auth = NULL;
        if(auth->scheme!=NULL)
            free(auth->scheme);
        deallocate_Buffer(&auth->auth);
        old_auth = auth;
        auth = auth->next;
        free(old_auth);
    }
    init_auth_info(auth_list);
}

int is_unrecoverable(zhandle_t *zh)
{
    return (zh->state<0)? ZINVALIDSTATE: ZOK;
}

zk_hashtable *exists_result_checker(zhandle_t *zh, int rc)
{
    if (rc == ZOK) {
        return zh->active_node_watchers;
    } else if (rc == ZNONODE) {
        return zh->active_exist_watchers;
    }
    return 0;
}

zk_hashtable *data_result_checker(zhandle_t *zh, int rc)
{
    return rc==ZOK ? zh->active_node_watchers : 0;
}

zk_hashtable *child_result_checker(zhandle_t *zh, int rc)
{
    return rc==ZOK ? zh->active_child_watchers : 0;
}

void close_zsock(zsock_t *fd)
{
    if (fd->sock != -1) {
#ifdef HAVE_OPENSSL_H
        if (fd->ssl_sock) {
            SSL_free(fd->ssl_sock);
            fd->ssl_sock = NULL;
            SSL_CTX_free(fd->ssl_ctx);
            fd->ssl_ctx = NULL;
        }
#endif
        close(fd->sock);
        fd->sock = -1;
    }
}

/**
 * Frees and closes everything associated with a handle,
 * including the handle itself.
 */
static void destroy(zhandle_t *zh)
{
    if (zh == NULL) {
        return;
    }
    /* call any outstanding completions with a special error code */
    cleanup_bufs(zh,1,ZCLOSING);
    if (process_async(zh->outstanding_sync)) {
        process_completions(zh);
    }
    if (zh->hostname != 0) {
        free(zh->hostname);
        zh->hostname = NULL;
    }
    if (zh->fd->sock != -1) {
        close_zsock(zh->fd);
        memset(&zh->addr_cur, 0, sizeof(zh->addr_cur));
        zh->state = 0;
    }
    addrvec_free(&zh->addrs);

    if (zh->chroot != NULL) {
        free(zh->chroot);
        zh->chroot = NULL;
    }
#ifdef HAVE_OPENSSL_H
    if (zh->fd->cert) {
        free(zh->fd->cert->certstr);
        free(zh->fd->cert);
        zh->fd->cert = NULL;
    }
#endif
    free_auth_info(&zh->auth_h);
    destroy_zk_hashtable(zh->active_node_watchers);
    destroy_zk_hashtable(zh->active_exist_watchers);
    destroy_zk_hashtable(zh->active_child_watchers);
    addrvec_free(&zh->addrs_old);
    addrvec_free(&zh->addrs_new);
	
	#ifdef HAVE_CYRUS_SASL_H
    if (zh->sasl_client) {
        zoo_sasl_client_destroy(zh->sasl_client);
        zh->sasl_client = NULL;
    }
#endif /* HAVE_CYRUS_SASL_H */
}

static void setup_random()
{
#ifndef _WIN32          // TODO: better seed
    int seed;
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd == -1) {
        seed = getpid();
    } else {
        int seed_len = 0;

        /* Enter a loop to fill in seed with random data from /dev/urandom.
         * This is done in a loop so that we can safely handle short reads
         * which can happen due to signal interruptions.
         */
        while (seed_len < sizeof(seed)) {
            /* Assert we either read something or we were interrupted due to a
             * signal (errno == EINTR) in which case we need to retry.
             */
            int rc = read(fd, &seed + seed_len, sizeof(seed) - seed_len);
            assert(rc > 0 || errno == EINTR);
            if (rc > 0) {
                seed_len += rc;
            }
        }
        close(fd);
    }
    srandom(seed);
    srand48(seed);
#endif
}

#ifndef __CYGWIN__
/**
 * get the errno from the return code
 * of get addrinfo. Errno is not set
 * with the call to getaddrinfo, so thats
 * why we have to do this.
 */
static int getaddrinfo_errno(int rc) {
    switch(rc) {
    case EAI_NONAME:
// ZOOKEEPER-1323 EAI_NODATA and EAI_ADDRFAMILY are deprecated in FreeBSD.
#if defined EAI_NODATA && EAI_NODATA != EAI_NONAME
    case EAI_NODATA:
#endif
        return ENOENT;
    case EAI_MEMORY:
        return ENOMEM;
    default:
        return EINVAL;
    }
}
#endif

/**
 * Count the number of hosts in the connection host string. This assumes it's
 * a well-formed connection string whereby each host is separated by a comma.
 */
static int count_hosts(char *hosts)
{
    uint32_t count = 0;
    char *loc = hosts;
    if (!hosts || strlen(hosts) == 0) {
        return 0;
    }

    while ((loc = strchr(loc, ','))) {
        count++;
        loc+=1;
    }

    return count+1;
}

/**
 * Resolve hosts and populate provided address vector with shuffled results.
 * The contents of the provided address vector will be initialized to an
 * empty state.
 */
static int resolve_hosts(const zhandle_t *zh, const char *hosts_in, addrvec_t *avec)
{
    int rc = ZOK;
    char *host = NULL;
    char *hosts = NULL;
    int num_hosts = 0;
    char *strtok_last = NULL;

    if (zh == NULL || hosts_in == NULL || avec == NULL) {
        return ZBADARGUMENTS;
    }

    // initialize address vector
    addrvec_init(avec);

    hosts = strdup(hosts_in);
    if (hosts == NULL) {
        LOG_ERROR(LOGCALLBACK(zh), "out of memory");
        errno=ENOMEM;
        rc=ZSYSTEMERROR;
        goto fail;
    }

    num_hosts = count_hosts(hosts);
    if (num_hosts == 0) {
        free(hosts);
        return ZOK;
    }

    // Allocate list inside avec
    rc = addrvec_alloc_capacity(avec, num_hosts);
    if (rc != 0) {
        LOG_ERROR(LOGCALLBACK(zh), "out of memory");
        errno=ENOMEM;
        rc=ZSYSTEMERROR;
        goto fail;
    }

    host = strtok_r(hosts, ",", &strtok_last);
    while(host) {
        char *port_spec = strrchr(host, ':');
        char *end_port_spec;
        int port;
        if (!port_spec) {
            LOG_ERROR(LOGCALLBACK(zh), "no port in %s", host);
            errno=EINVAL;
            rc=ZBADARGUMENTS;
            goto fail;
        }
        *port_spec = '\0';
        port_spec++;
        port = strtol(port_spec, &end_port_spec, 0);
        if (!*port_spec || *end_port_spec || port == 0) {
            LOG_ERROR(LOGCALLBACK(zh), "invalid port in %s", host);
            errno=EINVAL;
            rc=ZBADARGUMENTS;
            goto fail;
        }
#if defined(__CYGWIN__)
        // sadly CYGWIN doesn't have getaddrinfo
        // but happily gethostbyname is threadsafe in windows
        {
        struct hostent *he;
        char **ptr;
        struct sockaddr_in *addr4;

        he = gethostbyname(host);
        if (!he) {
            LOG_ERROR(LOGCALLBACK(zh), "could not resolve %s", host);
            errno=ENOENT;
            rc=ZBADARGUMENTS;
            goto fail;
        }

        // Setup the address array
        for(ptr = he->h_addr_list;*ptr != 0; ptr++) {
            if (addrs->count == addrs->capacity) {
                rc = addrvec_grow_default(addrs);
                if (rc != 0) {
                    LOG_ERROR(LOGCALLBACK(zh), "out of memory");
                    errno=ENOMEM;
                    rc=ZSYSTEMERROR;
                    goto fail;
                }
            }
            addr = &addrs->list[addrs->count];
            addr4 = (struct sockaddr_in*)addr;
            addr->ss_family = he->h_addrtype;
            if (addr->ss_family == AF_INET) {
                addr4->sin_port = htons(port);
                memset(&addr4->sin_zero, 0, sizeof(addr4->sin_zero));
                memcpy(&addr4->sin_addr, *ptr, he->h_length);
                zh->addrs.count++;
            }
#if defined(AF_INET6)
            else if (addr->ss_family == AF_INET6) {
                struct sockaddr_in6 *addr6;

                addr6 = (struct sockaddr_in6*)addr;
                addr6->sin6_port = htons(port);
                addr6->sin6_scope_id = 0;
                addr6->sin6_flowinfo = 0;
                memcpy(&addr6->sin6_addr, *ptr, he->h_length);
                zh->addrs.count++;
				}