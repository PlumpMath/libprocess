/* TODO(benh): Compile with a way to figure out which set of messages you used, and that way when someone with a different set of messages sends you a message you can declare that that message is not in your language of understanding. */
/* TODO(benh): Fix link functionality (processes need to send process_exit message since a dead process on one node might not know that a process on another node linked with it). */
/* TODO(benh): What happens when a remote link exits? Do we close the socket correclty?. */
/* TODO(benh): Revisit receive, pause, and await semantics. */
/* TODO(benh): Handle/Enable forking. */
/* TODO(benh): Use multiple processing threads (do process affinity). */
/* TODO(benh): Reclaim/Recycle stack (use Lithe!). */
/* TODO(benh): Better error handling (i.e., warn if re-spawn process). */
/* TODO(benh): Better protocol format checking in read_msg. */
/* TODO(benh): Use different backends for files and sockets. */
/* TODO(benh): Allow messages to be received out-of-order (i.e., allow
   someone to do a receive with a message id and let other messages
   queue until a message with that message id is received).  */

#include <assert.h>
#include <errno.h>
#include <ev.h>
#include <fcntl.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ucontext.h>
#include <unistd.h>

#include <arpa/inet.h>

#include <netinet/in.h>
#include <netinet/tcp.h>

#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <iostream>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>

#include "foreach.hpp"
#include "gate.hpp"
#include "process.hpp"
#include "singleton.hpp"

using std::cout;
using std::cerr;
using std::endl;
using std::list;
using std::map;
using std::make_pair;
using std::pair;
using std::queue;
using std::set;

#ifdef __sun__

#define gethostbyname2(name, _) gethostbyname(name)

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

#ifndef MAP_32BIT
#define MAP_32BIT 0
#endif

#endif /* __sun__ */

#define Byte (1)
#define Kilobyte (1024*Byte)
#define Megabyte (1024*Kilobyte)
#define Gigabyte (1024*Megabyte)
#define PROCESS_STACK_SIZE (64*Kilobyte)

#define malloc(bytes)                                               \
  ({ void *tmp; if ((tmp = malloc(bytes)) == NULL) abort(); tmp; })

#define realloc(address, bytes)                                     \
  ({ void *tmp; if ((tmp = realloc(address, bytes)) == NULL) abort(); tmp; })

/* Local server socket. */
static int s;

/* Global 'pipe' id uniquely assigned to each process. */
static uint32_t global_pipe = 0;

/* Local IP address. */
static uint32_t ip;

/* Local port. */
static uint16_t port;

/* Event loop. */
static struct ev_loop *loop;

/* Queue of new I/O watchers. */
static queue<ev_io *> *io_watchersq = new queue<ev_io *>();

/* Watcher queues mutex. */
static pthread_mutex_t watchersq_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Asynchronous watcher for interrupting loop. */
static ev_async async_watcher;

/* Timer watcher for process timeouts. */
static ev_timer timer_watcher;

/* Process timers mutex. */
static pthread_mutex_t timers_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Map (sorted) of process timers. */
typedef pair<Process *, int> timeout_t;
static map<ev_tstamp, list<timeout_t> > *timers =
  new map<ev_tstamp, list<timeout_t> >();

/* Flag to indicate whether or to update the timer on async interrupt. */
static bool update_timer = false;

/* Server watcher for accepting connections. */
static ev_io server_watcher;

/* I/O thread. */
static pthread_t io_thread;

/* Processing thread. */
static pthread_t proc_thread;

/* Initial scheduling context of processing thread. */
static ucontext_t proc_uctx_initial;

/* Running scheduling context of processing thread. */
static ucontext_t proc_uctx_running;

/* Current process of processing thread. */
static Process *proc_process;

/* Status of processing thread. */
static int idle = 0;

/* Scheduler gate. */
static Gate *gate = new Gate();

/* Status of infrastructure initialization (done lazily). */
static bool initialized = false;


struct write_ctx {
  int len;
  struct msg *msg;
  bool close;
};

struct read_ctx {
  int len;
  struct msg *msg;
};


void handle_await(struct ev_loop *loop, ev_io *w, int revents);
void read_msg(struct ev_loop *loop, ev_io *w, int revents);
static void write_msg(struct ev_loop *loop, ev_io *w, int revents);
static void write_connect(struct ev_loop *loop, ev_io *w, int revents);
static void link_connect(struct ev_loop *loop, ev_io *w, int revents);
void trampoline(int process0, int process1);


PID make_pid(const char *str)
{
  PID pid;
  std::istringstream iss(str);
  iss >> pid;
  return pid;
}


PID::operator std::string() const
{
  std::ostringstream oss;
  oss << *this;
  return oss.str();
}


bool PID::operator ! () const
{
  return !pipe && !ip && !port;
}


std::ostream& operator << (std::ostream& stream, const PID& pid)
{
  stream << pid.pipe << "@" << inet_ntoa(*((in_addr *) &pid.ip))
         << ":" << pid.port;
  return stream;
}


std::istream& operator >> (std::istream& stream, PID& pid)
{
  pid.pipe = 0;
  pid.ip = 0;
  pid.port = 0;

  std::string str;
  if (!(stream >> str)) {
    stream.setstate(std::ios_base::badbit);
    return stream;
  }

  if (str.size() > 500) {
    stream.setstate(std::ios_base::badbit);
    return stream;
  }

  char host[512];
  int id;
  unsigned short port;
  if (sscanf(str.c_str(), "%d@%[^:]:%hu", &id, host, &port) != 3) {
    stream.setstate(std::ios_base::badbit);
    return stream;
  }

  hostent *he = gethostbyname2(host, AF_INET);
  if (!he) {
    stream.setstate(std::ios_base::badbit);
    return stream;
  }

  pid.pipe = id;
  pid.ip = *((uint32_t *) he->h_addr);
  pid.port = port;
  return stream;
}


bool operator < (const PID& left, const PID& right)
{
  if (left.ip == right.ip && left.port == right.port)
    return left.pipe < right.pipe;
  else if (left.ip == right.ip && left.port != right.port)
    return left.port < right.port;
  else
    return left.ip < right.ip;
}


bool operator == (const PID& left, const PID& right)
{
  return (left.pipe == right.pipe &&
	  left.ip == right.ip &&
	  left.port == right.port);
}

static inline int set_nbio (int fd)
{
  int flags;

  /* If they have O_NONBLOCK, use the Posix way to do it */
#if defined(O_NONBLOCK)
  /* Fixme: O_NONBLOCK is defined but broken on SunOS 4.1.x and AIX 3.2.5. */
  if (-1 == (flags = fcntl(fd, F_GETFL, 0)))
    flags = 0;
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#else
  /* Otherwise, use the old way of doing it */
  flags = 1;
  return ioctl(fd, FIOBIO, &flags);
#endif
}

// {
//   int flags = 1;

//   if (ioctl(fd, FIONBIO, &flags) &&
//       ((flags = fcntl(fd, F_GETFL, 0)) < 0 ||
//        fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)) {
//     return -1;
//   }

//   return 0;
// }


struct node { uint32_t ip; uint16_t port; };

bool operator < (const node& left, const node& right)
{
  if (left.ip == right.ip)
    return left.port < right.port;
  else
    return left.ip < right.ip;
}

std::ostream& operator << (std::ostream& stream, const node& n)
{
  stream << n.ip << ":" << n.port;
  return stream;
}


class LinkManager : public Singleton<LinkManager>
{
private:
  /* Map from PID (local/remote) to process. */
  map<PID, set<Process *> > links;

  /* Map from socket to node (ip, port). */
  map<int, node> sockets;

  /* Maps from node (ip, port) to socket. */
  map<node, int> temps;
  map<node, int> persists;

  /* Map from socket to outgoing messages. */
  map<int, queue<struct msg *> > outgoing;

  pthread_mutex_t mutex;

  friend class Singleton<LinkManager>;

  LinkManager()
  {
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mutex, &attr);
    pthread_mutexattr_destroy(&attr);
  }

public:
  void link(Process *process, const PID &to)
  {
    //cout << "calling link" << endl;

    assert(process != NULL);

    node n = { to.ip, to.port };

    pthread_mutex_lock(&mutex);
    {
      // Check if node is remote and there isn't a persistant link.
      if ((n.ip != ip || n.port != port) &&
	  persists.find(n) == persists.end()) {
	int s;

	/* Create socket for communicating with remote process. */
	if ((s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
	  cerr << "failed to link (socket)" << endl;
	  abort();
	}
    
	/* Use non-blocking sockets. */
	if (set_nbio(s) < 0) {
	  cerr << "failed to link (set_nbio)" << endl;
	  abort();
	}

	//cout << "created linked socket " << s << endl;

	/* Record socket. */
	sockets[s] = n;

	/* Record node. */
	persists[n] = s;

	/* Allocate the watcher. */
	ev_io *io_watcher = (ev_io *) malloc(sizeof(ev_io));

	struct sockaddr_in addr;
      
	memset(&addr, 0, sizeof(addr));
      
	addr.sin_family = PF_INET;
	addr.sin_port = htons(to.port);
	addr.sin_addr.s_addr = to.ip;

	if (connect(s, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
	  if (errno != EINPROGRESS) {
	    cerr << "failed to link (connect)" << endl;
	    abort();
	  }

	  /* Initialize watcher for connecting. */
	  ev_io_init(io_watcher, link_connect, s, EV_WRITE);
	} else {
	  /* Initialize watcher for reading. */
	  io_watcher->data = malloc(sizeof(struct read_ctx));

	  /* Initialize read context. */
	  struct read_ctx *ctx = (struct read_ctx *) io_watcher->data;

	  ctx->len = 0;
	  ctx->msg = (struct msg *) malloc(sizeof(struct msg));

	  ev_io_init(io_watcher, read_msg, s, EV_READ);
	}

	/* Enqueue the watcher. */
	pthread_mutex_lock(&watchersq_mutex);
	{
	  io_watchersq->push(io_watcher);
	}
	pthread_mutex_unlock(&watchersq_mutex);

	/* Interrupt the loop. */
	ev_async_send(loop, &async_watcher);
      }

      links[to].insert(process);
    }
    pthread_mutex_unlock(&mutex);
  }

  void send(struct msg *msg)
  {
    assert(msg != NULL);

    //cout << "(1) sending msg to " << msg->to << endl;

    node n = { msg->to.ip, msg->to.port };

    pthread_mutex_lock(&mutex);
    {
      // Check if there is already a link.
      map<node, int>::iterator it;
      if ((it = persists.find(n)) != persists.end() ||
	  (it = temps.find(n)) != temps.end()) {
	int s = it->second;
	//cout << "(2) found a socket " << s << endl;
	if (outgoing.find(s) == outgoing.end()) {
	  assert(persists.find(n) != persists.end());
	  assert(temps.find(n) == temps.end());
	  //cout << "(3) reusing (sleeping persistant) socket " << s << endl;

	  /* Initialize the outgoing queue. */
	  outgoing[s];

	  /* Allocate/Initialize the watcher. */
	  ev_io *io_watcher = (ev_io *) malloc (sizeof (ev_io));

	  io_watcher->data = malloc(sizeof(struct write_ctx));

	  /* Initialize the write context. */
	  struct write_ctx *ctx = (struct write_ctx *) io_watcher->data;

	  ctx->len = 0;
	  ctx->msg = msg;
	  ctx->close = false;

	  ev_io_init(io_watcher, write_msg, s, EV_WRITE);

	  /* Enqueue the watcher. */
	  pthread_mutex_lock(&watchersq_mutex);
	  {
	    io_watchersq->push(io_watcher);
	  }
	  pthread_mutex_unlock(&watchersq_mutex);
    
	  /* Interrupt the loop. */
	  ev_async_send(loop, &async_watcher);
	} else {
	  //cout << "(3) reusing socket " << s << endl;
	  outgoing[s].push(msg);
	}
      } else {
	int s;

	/* Create socket for communicating with remote process. */
	if ((s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
	  cerr << "failed to send (socket)" << endl;
	  abort();
	}
    
	/* Use non-blocking sockets. */
	if (set_nbio(s) < 0) {
	  cerr << "failed to send (set_nbio)" << endl;
	  abort();
	}

	//cout << "(2) created temporary socket " << s << endl;

	/* Record socket. */
	sockets[s] = n;

	/* Record node. */
	temps[n] = s;

	/* Initialize the outgoing queue. */
	outgoing[s];

	/* Allocate/Initialize the watcher. */
	ev_io *io_watcher = (ev_io *) malloc (sizeof (ev_io));

	io_watcher->data = malloc(sizeof(struct write_ctx));

	/* Initialize the write context. */
	struct write_ctx *ctx = (struct write_ctx *) io_watcher->data;

	ctx->len = 0;
	ctx->msg = msg;
	ctx->close = true;

	struct sockaddr_in addr;
      
	memset(&addr, 0, sizeof(addr));
      
	addr.sin_family = PF_INET;
	addr.sin_port = htons(msg->to.port);
	addr.sin_addr.s_addr = msg->to.ip;
    
	if (connect(s, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
	  if (errno != EINPROGRESS) {
	    cerr << "failed to send (connect)" << endl;
	    abort();
	  }

	  /* Initialize watcher for connecting. */
	  ev_io_init(io_watcher, write_connect, s, EV_WRITE);
	} else {
	  /* Initialize watcher for writing. */
	  ev_io_init(io_watcher, write_msg, s, EV_WRITE);
	}
  
	/* Enqueue the watcher. */
	pthread_mutex_lock(&watchersq_mutex);
	{
	  io_watchersq->push(io_watcher);
	}
	pthread_mutex_unlock(&watchersq_mutex);
    
	/* Interrupt the loop. */
	ev_async_send(loop, &async_watcher);
      }
    }
    pthread_mutex_unlock(&mutex);
  }

  struct msg * next(int s)
  {
    struct msg *msg = NULL;
    pthread_mutex_lock(&mutex);
    {
      assert(outgoing.find(s) != outgoing.end());
      if (!outgoing[s].empty()) {
	msg = outgoing[s].front();
	outgoing[s].pop();
      }
    }
    pthread_mutex_unlock(&mutex);
    return msg;
  }

  struct msg * next_or_close(int s)
  {
    //cout << "next_or_close socket " << s << endl;
    struct msg *msg;
    pthread_mutex_lock(&mutex);
    {
      if ((msg = next(s)) == NULL) {
	assert(outgoing[s].empty());
	outgoing.erase(s);
	assert(temps.find(sockets[s]) != temps.end());
	temps.erase(sockets[s]);
	sockets.erase(s);
	::close(s);
      }
    }
    pthread_mutex_unlock(&mutex);
    return msg;
  }

  struct msg * next_or_sleep(int s)
  {
    //cout << "next_or_sleep socket " << s << endl;
    struct msg *msg;
    pthread_mutex_lock(&mutex);
    {
      if ((msg = next(s)) == NULL) {
	assert(outgoing[s].empty());
	outgoing.erase(s);
	assert(persists.find(sockets[s]) != persists.end());
      }
    }
    pthread_mutex_unlock(&mutex);
    return msg;
  }

  void closed(int s)
  {
    //cout << "closed socket " << s << endl;
    pthread_mutex_lock(&mutex);
    {
      map<int, node>::iterator it = sockets.find(s);
      if (it != sockets.end()) {
	exited(it->second);
	persists.erase(sockets[s]);
	temps.erase(sockets[s]);
	sockets.erase(s);
	outgoing.erase(s);
	::close(s);
      }
    }
    pthread_mutex_unlock(&mutex);
  }

  void exited(const node &n)
  {
    pthread_mutex_lock(&mutex);
    {
      list<PID> removed;
      /* Look up all linked processes. */
      foreachpair (const PID &pid, set<Process *> processes, links) {
	if (pid.ip == n.ip && pid.port == n.port) {
	  /* N.B. If we call exited(pid) we might invalidate iteration. */
	  /* Deliver PROCESS_EXIT messages. */
	  foreach (Process *process, processes) {
	    struct msg *msg = (struct msg *) malloc(sizeof(struct msg));
	    msg->from.pipe = pid.pipe;
	    msg->from.ip = pid.ip;
	    msg->from.port = pid.port;
	    msg->to.pipe = process->pid.pipe;
	    msg->to.ip = process->pid.ip;
	    msg->to.port = process->pid.port;
	    msg->id = PROCESS_EXIT;
	    msg->len = 0;
	    process->enqueue(*msg);
	  }
	  removed.push_back(pid);
	}
      }
      foreach (const PID &pid, removed)
	links.erase(pid);
    }
    pthread_mutex_unlock(&mutex);
  }

  void exited(const PID &pid)
  {
    pthread_mutex_lock(&mutex);
    {
      /* Look up all linked processes. */
      map<PID, set<Process *> >::iterator it = links.find(pid);

      if (it != links.end()) {
	set<Process *> processes = it->second;
	/* Deliver PROCESS_EXIT messages. */
	foreach (Process *process, processes) {
	  struct msg *msg = (struct msg *) malloc(sizeof(struct msg));
	  msg->from.pipe = pid.pipe;
	  msg->from.ip = pid.ip;
	  msg->from.port = pid.port;
	  msg->to.pipe = process->pid.pipe;
	  msg->to.ip = process->pid.ip;
	  msg->to.port = process->pid.port;
	  msg->id = PROCESS_EXIT;
	  msg->len = 0;
	  process->enqueue(*msg);
	}
	links.erase(pid);
      }
    }
    pthread_mutex_unlock(&mutex);
  }
};

/* Singleton LinkManager instance. */
template<> LinkManager * Singleton<LinkManager>::singleton = NULL;
template<> bool Singleton<LinkManager>::instantiated = false;


class ProcessManager : public Singleton<ProcessManager>
{
private:
  /* Map of all local spawned and running processes. */
  map<int, Process *> processes;

  /* Map of all waiting processes. */
  map<Process *, list<Process *> > waiters;

  /* Map of gates for waiting threads. */
  map<Process *, Gate *> gates;

  /* Processes mutex. */
  pthread_mutex_t processes_mutex;

  /* Queue of runnable processes. */
  queue<Process *> runq;

  /* Run queue mutex. */
  pthread_mutex_t runq_mutex;

  friend class Singleton<ProcessManager>;

  ProcessManager()
  {
    pthread_mutex_init(&processes_mutex, NULL);
    pthread_mutex_init(&runq_mutex, NULL);
  }

public:
  void run (Process &process)
  {
    /* pthread_mutex_lock(&proces.mutex); */
    {
      process.state = Process::RUNNING;
    }
    pthread_mutex_unlock(&process.mutex);

    try {
      process();
    } catch (const std::exception &e) {
      std::cerr << "libprocess: " << process.pid
		<< " exited due to "
		<< e.what() << std::endl;
    } catch (...) {
      std::cerr << "libprocess: " << process.pid
		<< " exited due to unknown exception" << std::endl;
    }

    process.state = Process::EXITED;
    cleanup(process);
    setcontext(&proc_uctx_initial);
  }

  void receive (Process &process, time_t secs)
  {
    pthread_mutex_lock (&process.mutex);
    {
      /* Ensure nothing enqueued since check in Process::receive. */
      if (process.msgs.empty()) {
	if (secs > 0) {
	  /* Initialize the timeout. */
	  ev_tstamp tstamp = ev_time() + secs;

	  /* Create timeout pair. */
	  pair<Process *, int> timeout =
	    make_pair(&process, process.generation);

	  /* Add the timer. */
	  pthread_mutex_lock(&timers_mutex);
	  {
	    (*timers)[tstamp].push_back(timeout);

	    /* Interrupt the loop if there isn't an adequate timer running. */
	    if (timers->size() == 1 || tstamp < timers->begin()->first) {
	      update_timer = true;
	      ev_async_send(loop, &async_watcher);
	    }
	  }
	  pthread_mutex_unlock(&timers_mutex);

	  /* Context switch. */
	  process.state = Process::RECEIVING;
	  swapcontext(&process.uctx, &proc_uctx_running);

// 	assert(process.state == Process::TIMEDOUT ||
// 	       process.state == Process::READY);

	  if (process.state != Process::TIMEDOUT) {
	    /* Attempt to cancel the timer. */
	    pthread_mutex_lock(&timers_mutex);
	    {
	      if (timers->find(tstamp) != timers->end()) {
		(*timers)[tstamp].remove(timeout);

		if ((*timers)[tstamp].empty())
		  timers->erase(tstamp);
	      }
	      /* N.B. We don't reset timer, so possible unnecessary timeouts. */
	    }
	    pthread_mutex_unlock(&timers_mutex);
	  }

	  process.state = Process::RUNNING;
      
	  /* Update the generation (handles racing timeouts). */
	  process.generation++;
	} else {
	  /* Context switch. */
	  process.state = Process::RECEIVING;
	  swapcontext(&process.uctx, &proc_uctx_running);
	  assert(process.state == Process::READY);
	  process.state = Process::RUNNING;
	}
      }
    }
    pthread_mutex_unlock (&process.mutex);
  }

  void pause (Process &process, time_t secs)
  {
    pthread_mutex_lock (&process.mutex);
    {
      if (secs > 0) {
	/* Initialize the timeout. */
	ev_tstamp tstamp = ev_time() + secs;

	/* Create timeout pair. */
	pair<Process *, int> timeout =
	  pair<Process *, int>(&process, process.generation);

	/* Add the timer. */
	pthread_mutex_lock(&timers_mutex);
	{
	  (*timers)[tstamp].push_back(timeout);
	  
	  /* Interrupt the loop if there isn't an adequate timer running. */
	  if (timers->size() == 1 || tstamp < timers->begin()->first) {
	    update_timer = true;
	    ev_async_send(loop, &async_watcher);
	  }
	}
	pthread_mutex_unlock(&timers_mutex);

	/* Context switch. */
	process.state = Process::PAUSED;
	swapcontext(&process.uctx, &proc_uctx_running);
	assert(process.state == Process::TIMEDOUT);
	process.state = Process::RUNNING;

	/* Update the generation (for posterity). */
	process.generation++;
      }
    }
    pthread_mutex_unlock (&process.mutex);
  }

  void timeout (Process *process, int generation)
  {
    assert(process != NULL);
    pthread_mutex_lock(&process->mutex);
    {
      /* N.B. State != READY after timeout, but generation still same. */
      if (process->state != Process::READY &&
	  process->generation == generation) {
	/* N.B. Process may be RUNNING due to "outside" thread 'receive'. */
	assert(process->state == Process::RUNNING ||
	       process->state == Process::RECEIVING ||
	       process->state == Process::PAUSED);
	process->state = Process::TIMEDOUT;
	if (process->state != Process::RUNNING)
	  ProcessManager::instance()->enqueue(*process);
      }
    }
    pthread_mutex_unlock(&process->mutex);
  }
  
  void cleanup (Process &process)
  {
    /* TODO(benh): Cleanup 'msgs' queue. */

    /* Free current message. */
    if (process.current) free(process.current);

    /* TODO(benh): Reclaim/Recycle stack (use Lithe!). */

    /* Remove process. */
    pthread_mutex_lock(&processes_mutex);
    {
      processes.erase(process.pid.pipe);

      /* Wake up any waiting processes. */
      foreach (Process *waiter, waiters[&process])
	enqueue(*waiter);

      waiters.erase(&process);

      /* Wake up any waiting threads. */
      map<Process *, Gate *>::iterator it = gates.find(&process);
      if (it != gates.end())
	it->second->open();

      /* N.B. CAN'T use 'process'; it might already be cleaned up by client. */

      gates.erase(&process);
    }
    pthread_mutex_unlock(&processes_mutex);

    /* Inform link manager. */
    LinkManager::instance()->exited(process.pid);
  }
  
  void spawn(Process *process)
  {
    assert(process != NULL);

    /* Set up the ucontext. */
    if (getcontext(&process->uctx) < 0)
      abort();

    process->state = Process::READY;

    const int protection = (PROT_READ | PROT_WRITE);
    const int flags = (MAP_PRIVATE | MAP_ANONYMOUS | MAP_32BIT);

    void *stack = mmap(NULL, PROCESS_STACK_SIZE, protection, flags, -1, 0);

    if (stack == MAP_FAILED)
      abort();

    /* Disallow all memory access to the last page. */
    if (mprotect(stack, getpagesize(), PROT_NONE) != 0)
      abort();
    
    process->uctx.uc_stack.ss_sp = stack;
    process->uctx.uc_stack.ss_size = PROCESS_STACK_SIZE;
    process->uctx.uc_link = 0;

    /* Package the arguments. */
#ifdef __x86_64__
    assert(sizeof(unsigned long) == sizeof(Process *));
    int process0 = (unsigned int) (unsigned long) process;
    int process1 = (unsigned long) process >> 32;
#else
    assert(sizeof(unsigned int) == sizeof(Process *));
    int process0 = (unsigned int) process;
    int process1 = 0;
#endif /* __x86_64__ */

    makecontext(&process->uctx, (void (*)()) trampoline, 2, process0, process1);
    /* Record process. */
    pthread_mutex_lock(&processes_mutex);
    {
      processes[process->pid.pipe] = process;
    }
    pthread_mutex_unlock(&processes_mutex);

    /* Add process to the run queue. */
    enqueue(*process);
  }

  void wait (const PID &pid)
  {
    pthread_mutex_lock(&processes_mutex);
    {
      Process *process;
      map<int, Process *>::iterator iter = processes.find(pid.pipe);
      if (iter != processes.end()) {
	process = iter->second;
	if (process->state != Process::EXITED) {
	  if (pthread_self() != proc_thread) {
	    if (gates.find(process) == gates.end())
	      gates[process] = new Gate();
	    Gate *gate = gates[process];
	    Gate::state_t old = gate->approach();
	    pthread_mutex_unlock(&processes_mutex);
	    gate->arrive(old);
	    if (gate->empty())
	      delete gate;
	    return;
	  } else {
	    waiters[process].push_back(proc_process);
	    pthread_mutex_unlock(&processes_mutex);
	    pthread_mutex_lock(&proc_process->mutex);
	    {
	      /* Context switch. */
	      proc_process->state = Process::WAITING;
	      swapcontext(&proc_process->uctx, &proc_uctx_running);
	      assert(proc_process->state == Process::WAITING);
	      proc_process->state = Process::RUNNING;
	    }
	    pthread_mutex_unlock(&proc_process->mutex);
	    return;
	  }
	}
      }
    }
    pthread_mutex_unlock(&processes_mutex);
  }

  void await (Process *process, int fd, int op)
  {
    assert(process != NULL);

    if (fd < 0)
      return;

    /* Allocate/Initialize the watcher. */
    ev_io *io_watcher = (ev_io *) malloc (sizeof (ev_io));

    io_watcher->data = new pair<Process *, int>(process, process->generation);

    /* Initialize watcher. */
    if ((op & Process::RDWR) == Process::RDWR)
      ev_io_init(io_watcher, handle_await, fd, EV_READ | EV_WRITE);
    else if ((op & Process::RDONLY) == Process::RDONLY)
      ev_io_init(io_watcher, handle_await, fd, EV_READ);
    else if ((op & Process::WRONLY) == Process::WRONLY)
      ev_io_init(io_watcher, handle_await, fd, EV_WRITE);

    pthread_mutex_lock(&process->mutex);
    {
      /* Enqueue the watcher. */
      pthread_mutex_lock(&watchersq_mutex);
      {
	io_watchersq->push(io_watcher);
      }
      pthread_mutex_unlock(&watchersq_mutex);
    
      /* Interrupt the loop. */
      ev_async_send(loop, &async_watcher);

      /* Context switch. */
      process->state = Process::AWAITING;
      swapcontext(&process->uctx, &proc_uctx_running);
      assert(process->state == Process::READY);
      process->state = Process::RUNNING;
      
      /* Update the generation (handles racing awaited). */
      process->generation++;
    }
    pthread_mutex_unlock(&process->mutex);
  }

  void awaited (Process &process, int generation)
  {
    pthread_mutex_lock(&process.mutex);
    {
      if (process.generation == generation) {
	assert(process.state == Process::AWAITING);
	process.state = Process::READY;
	enqueue(process);
      }
    }
    pthread_mutex_unlock(&process.mutex);    
  }

  void kill (Process &process)
  {
    process.state = Process::EXITED; // s/EXITED/KILLED ?
    cleanup(process);
    setcontext(&proc_uctx_initial);
  }

  void enqueue (Process &process)
  {
    pthread_mutex_lock (&runq_mutex);
    {
      runq.push (&process);
    }
    pthread_mutex_unlock (&runq_mutex);
    
    /* Wake up the processing thread if necessary. */
    gate->open();
  }

  Process * dequeue ()
  {
    Process *process = NULL;

    pthread_mutex_lock (&runq_mutex);
    {
      if (!runq.empty()) {
	process = runq.front ();
	runq.pop ();
      }
    }
    pthread_mutex_unlock (&runq_mutex);

    return process;
  }

  void deliver (struct msg &msg)
  {
//     cout << endl;
//     cout << "msg.from.pipe: " << msg.from.pipe << endl;
//     cout << "msg.from.ip: " << msg.from.ip << endl;
//     cout << "msg.from.port: " << msg.from.port << endl;
//     cout << "msg.to.pipe: " << msg.to.pipe << endl;
//     cout << "msg.to.ip: " << msg.to.ip << endl;
//     cout << "msg.to.port: " << msg.to.port << endl;
//     cout << "msg.id: " << msg.id << endl;
//     cout << "msg.len: " << msg.len << endl;

    Process *process = NULL;

    pthread_mutex_lock (&processes_mutex);
    {
      map<int, Process *>::iterator iter = processes.find(msg.to.pipe);
      if (iter != processes.end()) {
	process = iter->second;
      }
    }
    pthread_mutex_unlock (&processes_mutex);

    /* TODO(benh): What if process is cleaned up right here! */

    if (process != NULL) {
      process->enqueue(msg);
    } else {
      free(&msg);
    }
  }
};

/* Singleton ProcessManager instance. */
template<> ProcessManager * Singleton<ProcessManager>::singleton = NULL;
template<> bool Singleton<ProcessManager>::instantiated = false;


static void async (struct ev_loop *loop, ev_async *w, int revents)
{
  pthread_mutex_lock (&watchersq_mutex);
  {
    /* Start all the new I/O watchers. */
    while (!io_watchersq->empty()) {
      ev_io *io_watcher = io_watchersq->front();
      io_watchersq->pop();
      ev_io_start(loop, io_watcher);
    }
  }
  pthread_mutex_unlock (&watchersq_mutex);

  pthread_mutex_lock(&timers_mutex);
  {
    if (update_timer) {
      if (!timers->empty()) {
	timer_watcher.repeat = timers->begin()->first - ev_now(loop) > 0 ? : 0;
	if (timer_watcher.repeat == 0) {
	  ev_feed_event(loop, &timer_watcher, EV_TIMEOUT);
	} else {
	  ev_timer_again(loop, &timer_watcher);
	}
      }
      update_timer = false;
    }
  }
  pthread_mutex_unlock(&timers_mutex);
}


void handle_await (struct ev_loop *loop, ev_io *w, int revents)
{
  pair<Process *, int> *p = (pair<Process *, int> *) w->data;

  ProcessManager::instance()->awaited(*p->first, p->second);

  ev_io_stop(loop, w);

  delete p;

  free(w);
}


void read_data (struct ev_loop *loop, ev_io *w, int revents)
{
  int c = w->fd;
  //cout << "read_data on " << c << " started" << endl;

  struct read_ctx *ctx = (struct read_ctx *) w->data;

  /* Read the data starting from the last read. */
  int len = recv(c,
		 (char *) ctx->msg + sizeof(struct msg) + ctx->len,
		 ctx->msg->len - ctx->len,
		 0);

  if (len > 0) {
    ctx->len += len;
  } else if (len < 0 && errno == EWOULDBLOCK) {
    return;
  } else if (len == 0 || (len < 0 &&
			  (errno == ECONNRESET ||
			   errno == EBADF ||
			   errno == EHOSTUNREACH))) {
    /* Socket has closed. */
    perror("libprocess recv error: ");
    //cout << "read_data: closing socket " << c << endl;
    LinkManager::instance()->closed(c);

    /* Stop receiving ... */
    ev_io_stop (loop, w);
    close(c);
    free(ctx->msg);
    free(ctx);
    free(w);
    return;
  } else {
    perror("unhandled socket error: please report (read_data)");
    abort();
  }

  if (ctx->len == ctx->msg->len) {
    /* Deliver message. */
    ProcessManager::instance()->deliver(*ctx->msg);

    /* Reinitialize read context. */
    ctx->len = 0;
    ctx->msg = (struct msg *) malloc(sizeof(struct msg));

    //cout << "read_data on " << c << " finished" << endl;

    /* Continue receiving ... */
    ev_io_stop (loop, w);
    ev_io_init (w, read_msg, c, EV_READ);
    ev_io_start (loop, w);
  }
}


void read_msg (struct ev_loop *loop, ev_io *w, int revents)
{
  int c = w->fd;
  //cout << "read_msg on " << c << " started" << endl;

  struct read_ctx *ctx = (struct read_ctx *) w->data;

  /* Read the message starting from the last read. */
  int len = recv(c,
		 (char *) ctx->msg + ctx->len,
		 sizeof (struct msg) - ctx->len,
		 0);

  if (len > 0) {
    ctx->len += len;
  } else if (len < 0 && errno == EWOULDBLOCK) {
    return;
  } else if (len == 0 || (len < 0 &&
			  (errno == ECONNRESET ||
			   errno == EBADF ||
			   errno == EHOSTUNREACH))) {
    /* Socket has closed. */
    perror("libprocess recv error: ");
    //cout << "read_msg: closing socket " << c << endl;
    LinkManager::instance()->closed(c);

    /* Stop receiving ... */
    ev_io_stop (loop, w);
    close(c);
    free(ctx->msg);
    free(ctx);
    free(w);
    return;
  } else {
    perror("unhandled socket error: please report (read_msg)");
    abort();
  }

  if (ctx->len == sizeof(struct msg)) {
    /* Check and see if we need to receive data. */
    if (ctx->msg->len > 0) {
      /* Allocate enough space for data. */
      ctx->msg = (struct msg *)
	realloc (ctx->msg, sizeof(struct msg) + ctx->msg->len);

      /* TODO(benh): Optimize ... try doing a read first! */
      ctx->len = 0;

      /* Start receiving data ... */
      ev_io_stop (loop, w);
      ev_io_init (w, read_data, c, EV_READ);
      ev_io_start (loop, w);
    } else {
      /* Deliver message. */
      //cout << "delivering message" << endl;
      ProcessManager::instance()->deliver(*ctx->msg);

      /* Reinitialize read context. */
      ctx->len = 0;
      ctx->msg = (struct msg *) malloc(sizeof(struct msg));

      /* Continue receiving ... */
      ev_io_stop (loop, w);
      ev_io_init (w, read_msg, c, EV_READ);
      ev_io_start (loop, w);
    }
  }
}


static void write_data (struct ev_loop *loop, ev_io *w, int revents)
{
  int c = w->fd;

  //cout << "write_data on " << c << " started" << endl;

  struct write_ctx *ctx = (struct write_ctx *) w->data;

  int len = send(c,
		 (char *) ctx->msg + sizeof(struct msg) + ctx->len,
		 ctx->msg->len - ctx->len,
		 MSG_NOSIGNAL);

  if (len > 0) {
    ctx->len += len;
  } else if (len < 0 && errno == EWOULDBLOCK) {
    return;
  } else if (len == 0 || (len < 0 &&
			  (errno == ECONNRESET ||
			   errno == EBADF ||
			   errno == EHOSTUNREACH ||
			   errno == EPIPE))) {
    /* Socket has closed. */
    perror("libprocess send error: ");
    //cout << "write_data: closing socket " << c << endl;
    LinkManager::instance()->closed(c);

    /* Stop receiving ... */
    ev_io_stop (loop, w);
    close(c);
    free(ctx->msg);
    free(ctx);
    free(w);
    return;
  } else {
    perror("unhandled socket error: please report (write_data)");
    abort();
  }

  if (ctx->len == ctx->msg->len) {
    ev_io_stop (loop, w);
    free(ctx->msg);

    if (ctx->close)
      ctx->msg = LinkManager::instance()->next_or_close(c);
    else
      ctx->msg = LinkManager::instance()->next_or_sleep(c);

    if (ctx->msg != NULL) {
      ctx->len = 0;
      ev_io_init(w, write_msg, c, EV_WRITE);
      ev_io_start(loop, w);
    } else {
      //cout << "write_data on " << c << " finished" << endl;
      free(ctx);
      free(w);
    }
  }
}


static void write_msg(struct ev_loop *loop, ev_io *w, int revents)
{
  int c = w->fd;
  //cout << "write_msg on " << c << " started" << endl;

  struct write_ctx *ctx = (struct write_ctx *) w->data;

  int len = send(c,
		 (char *) ctx->msg + ctx->len,
		 sizeof (struct msg) - ctx->len,
		 MSG_NOSIGNAL);


  if (len > 0) {
    ctx->len += len;
  } else if (len < 0 && errno == EWOULDBLOCK) {
    return;
  } else if (len == 0 || (len < 0 &&
			  (errno == ECONNRESET ||
			   errno == EBADF ||
			   errno == EHOSTUNREACH ||
			   errno == EPIPE))) {
    /* Socket has closed. */
    perror("libprocess send error: ");
    //cout << "write_msg: closing socket " << c << endl;
    LinkManager::instance()->closed(c);

    /* Stop receiving ... */
    ev_io_stop (loop, w);
    close(c);
    free(ctx->msg);
    free(ctx);
    free(w);
    return;
  } else {
    perror("unhandled socket error: please report (write_msg)");
    abort();
  }

  if (ctx->len == sizeof(struct msg)) {
    /* Check and see if we need to write data. */
    if (ctx->msg->len > 0) {
      
      /* TODO(benh): Optimize ... try doing a write first! */
      ctx->len = 0;

      /* Start writing data ... */
      ev_io_stop(loop, w);
      ev_io_init(w, write_data, c, EV_WRITE);
      ev_io_start(loop, w);
    } else {
      //cout << "write_msg: closing socket" << endl;
      ev_io_stop(loop, w);
      free(ctx->msg);

      if (ctx->close)
	ctx->msg = LinkManager::instance()->next_or_close(c);
      else
	ctx->msg = LinkManager::instance()->next_or_sleep(c);

      if (ctx->msg != NULL) {
	ctx->len = 0;
	ev_io_init(w, write_msg, c, EV_WRITE);
	ev_io_start(loop, w);
      } else {
	//cout << "write_msg on " << c << " finished" << endl;
	free(ctx);
	free(w);
      }
    }
  }
}


static void write_connect (struct ev_loop *loop, ev_io *w, int revents)
{
  //cout << "write_connect" << endl;
  int s = w->fd;

  struct write_ctx *ctx = (struct write_ctx *) w->data;

  ev_io_stop(loop, w);

  /* Check that the connection was successful. */
  int opt;
  socklen_t optlen = sizeof(opt);

  if (getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0) {
    //cerr << "failed to connect (getsockopt)" << endl;
    LinkManager::instance()->closed(s);
    free(ctx->msg);
    free(ctx);
    free(w);
    return;
  }

  if (opt != 0) {
    //cerr << "failed to connect" << endl;
    LinkManager::instance()->closed(s);
    free(ctx->msg);
    free(ctx);
    free(w);
    return;
  }

  /* TODO(benh): Optimize ... try doing a write first. */

  ev_io_init(w, write_msg, s, EV_WRITE);
  ev_io_start(loop, w);
}



static void link_connect (struct ev_loop *loop, ev_io *w, int revents)
{
  //cout << "link_connect" << endl;
  int s = w->fd;

  ev_io_stop(loop, w);

  /* Check that the connection was successful. */
  int opt;
  socklen_t optlen = sizeof(opt);

  if (getsockopt(s, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0) {
    //cerr << "failed to connect (getsockopt)" << endl;
    LinkManager::instance()->closed(s);
    free(w);
    return;
  }

  if (opt != 0) {
    //cerr << "failed to connect" << endl;
    LinkManager::instance()->closed(s);
    free(w);
    return;
  }

  /* Reuse/Initialize the watcher. */
  w->data = malloc(sizeof(struct read_ctx));

  /* Initialize read context. */
  struct read_ctx *ctx = (struct read_ctx *) w->data;

  ctx->len = 0;
  ctx->msg = (struct msg *) malloc(sizeof(struct msg));

  /* Initialize watcher for reading. */
  ev_io_init(w, read_msg, s, EV_READ);

  ev_io_start(loop, w);
}


void timeout (struct ev_loop *loop, ev_timer *w, int revents)
{
  list<timeout_t> timedout;

  pthread_mutex_lock(&timers_mutex);
  {
    ev_tstamp now = ev_now(loop);

    map<ev_tstamp, list<timeout_t> >::iterator it = timers->begin();
    map<ev_tstamp, list<timeout_t> >::iterator last = timers->begin();

    for (; it != timers->end(); ++it) {
      // Check if timer has expired.
      ev_tstamp tstamp = it->first;
      if (tstamp > now) {
	last = it;
	break;
      }

      // Save all expired timeouts.
      list<timeout_t> &timeouts = it->second;
      foreach (const timeout_t &timeout, timeouts)
	timedout.push_back(timeout);
    }

    if (it == timers->end())
      timers->clear();
    else if (last != timers->begin())
      timers->erase(timers->begin(), last);

    if (!timers->empty()) {
      timer_watcher.repeat = timers->begin()->first - now;
      assert(timer_watcher.repeat > 0);
      ev_timer_again(loop, &timer_watcher);
    } else {
      timer_watcher.repeat = 0.;
      ev_timer_again(loop, &timer_watcher);
    }

    update_timer = false;
  }
  pthread_mutex_unlock(&timers_mutex);

  foreach (const timeout_t &timeout, timedout) {
    assert(timeout.first != NULL);
    ProcessManager::instance()->timeout(timeout.first, timeout.second);
  }
}


static void handle_accept (struct ev_loop *loop, ev_io *w, int revents)
{
  //cout << "handle_accept" << endl;
  int s = w->fd;

  struct sockaddr_in addr;

  socklen_t addrlen = sizeof(addr);

  /* Do accept. */
  int c = accept(s, (struct sockaddr *) &addr, &addrlen);

  if (c < 0) {
    return;
  }

  /* Make socket non-blocking. */
  if (set_nbio(c) < 0) {
    close(c);
    return;
  }

  /* Turn off Nagle (on TCP_NODELAY) so pipelined requests don't wait. */
  int on = 1;
  if (setsockopt(c, SOL_TCP, TCP_NODELAY, &on, sizeof(on)) < 0) {
    close(c);
    return;
  }

  /* Allocate the watcher. */
  ev_io *io_watcher = (ev_io *) malloc (sizeof (ev_io));

  io_watcher->data = malloc(sizeof(struct read_ctx));

  /* Initialize the read context */
  struct read_ctx *ctx = (struct read_ctx *) io_watcher->data;

  ctx->len = 0;
  ctx->msg = (struct msg *) malloc(sizeof(struct msg));

  /* Initialize watcher for reading. */
  ev_io_init(io_watcher, read_msg, c, EV_READ);

  ev_io_start(loop, io_watcher);
}


static void * node (void *arg)
{
  ev_loop(((struct ev_loop *) arg), 0);

  return NULL;
}


void * schedule (void *arg)
{
  if (getcontext(&proc_uctx_initial) < 0) {
    cerr << "failed to schedule (getcontext)" << endl;
    abort();
  }

  do {
    //cout << "...before 1st dequeue..." << endl;
    Process *process = ProcessManager::instance()->dequeue();

    if (process == NULL) {
      //cout << "...before gate->approach..." << endl;
      Gate::state_t old = gate->approach();
      //cout << "...before 2nd dequeue..." << endl;
      process = ProcessManager::instance()->dequeue();
      if (process == NULL) {
	/* Wait at gate if idle. */
	//cout << "...before gate->arrive..." << endl;
	gate->arrive(old);
	continue;
      } else {
	//cout << "...before gate->leave..." << endl;
	gate->leave();
      }
    }

    //cout << "...before &process->mutex (" << &process->mutex << ")..." << endl;
    pthread_mutex_lock (&process->mutex);
    {
      assert(process->state == Process::READY ||
	     process->state == Process::TIMEDOUT ||
	     process->state == Process::WAITING);

      /* Continue process. */
      proc_process = process;
      swapcontext(&proc_uctx_running, &process->uctx);
      proc_process = NULL;
    }
    pthread_mutex_unlock (&process->mutex);
  } while (true);
}


void trampoline (int process0, int process1)
{
  /* Unpackage the arguments. */
#ifdef __x86_64__
  assert (sizeof(unsigned long) == sizeof(Process *));
  Process *process = (Process *)
    (((unsigned long) process1 << 32) + (unsigned int) process0);
#else
  assert (sizeof(unsigned int) == sizeof(Process *));
  Process *process = (Process *) (unsigned int) process0;
#endif /* __x86_64__ */

  /* Run the process. */
  ProcessManager::instance()->run(*process);
}


void sigsev (int signal, struct sigcontext *ctx)
{
  if (pthread_self() != proc_thread) {
    /* Pass on the signal (so that a core file is produced).  */
    struct sigaction sa;
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(signal, &sa, NULL);
    raise(signal);
  } else {
    /* TODO(benh): Need to use setjmp/longjmp for error handling. */
    ProcessManager::instance()->kill(*proc_process);
  }
}


static void initialize ()
{
  /* Install signal handler. */
  struct sigaction sa;

  sa.sa_handler = (void (*) (int)) sigsev;
  sigemptyset (&sa.sa_mask);
  sa.sa_flags = SA_RESTART;

//   sigaction (SIGSEGV, &sa, NULL);

//   sigaction (SIGILL, &sa, NULL);
// #ifdef SIGBUS
//   sigaction (SIGBUS, &sa, NULL);
// #endif
// #ifdef SIGSTKFLT
//   sigaction (SIGSTKFLT, &sa, NULL);
// #endif
//   sigaction (SIGABRT, &sa, NULL);
//   sigaction (SIGFPE, &sa, NULL);

#ifdef __sun__
  signal(SIGPIPE, SIG_IGN);
#endif /* __sun__ */

  /* Setup processing thread. */
  if (pthread_create (&proc_thread, NULL, schedule, NULL) != 0) {
    cerr << "failed to initialize (pthread_create)" << endl;
    abort();
  }

  char *value;

  /* Check environment for ip. */
  value = getenv("LIBPROCESS_IP");
  ip = value != NULL ? atoi(value) : 0;

  /* Check environment for port. */
  value = getenv("LIBPROCESS_PORT");
  port = value != NULL ? atoi(value) : 0;

  // Lookup hostname if missing ip (avoids getting 127.0.0.1). Note
  // that we need only one ip address, so that other processes can
  // send and receive and don't get confused as to whom they are
  // sending to.
  if (ip == 0) {
    char hostname[512];

    if (gethostname(hostname, sizeof(hostname)) < 0) {
      cerr << "failed to initialize (gethostname)" << endl;
      abort();
    }

    /* Lookup IP address of local hostname. */
    struct hostent *he;

    if ((he = gethostbyname2(hostname, AF_INET)) == NULL) {
      cerr << "failed to initialize (gethostbyname2)" << endl;
      abort();
    }

    ip = *((uint32_t *) he->h_addr_list[0]);
  }

  /* Create a "server" socket for communicating with other nodes. */
  if ((s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
    cerr << "failed to initialize (socket)" << endl;
    abort();
  }

  /* Make socket non-blocking. */
  if (set_nbio(s) < 0) {
    cerr << "failed to initialize (set_nbio)" << endl;
    abort();
  }

  /* Set up socket. */
  struct sockaddr_in addr;
  addr.sin_family = PF_INET;
  addr.sin_addr.s_addr = ip;
  addr.sin_port = htons(port);

  if (bind(s, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
    cerr << "failed to initialize (bind)" << endl;
    abort();
  }

  /* Lookup and record assigned ip and assigned port. */
  socklen_t addrlen = sizeof(addr);
  if (getsockname(s, (struct sockaddr *) &addr, &addrlen) < 0) {
    cerr << "failed to initialize (getsockname)" << endl;
    abort();
  }

  ip = addr.sin_addr.s_addr;
  port = ntohs(addr.sin_port);

  if (listen(s, 500000) < 0) {
    cerr << "failed to initialize (listen)" << endl;
    abort();
  }

  /* Setup event loop. */
#ifdef __sun__
  loop = ev_default_loop(EVBACKEND_POLL | EVBACKEND_SELECT);
#else
  loop = ev_default_loop(EVFLAG_AUTO);
#endif /* __sun__ */

  ev_async_init (&async_watcher, async);
  ev_async_start (loop, &async_watcher);

  ev_timer_init (&timer_watcher, timeout, 0., 2100000.0);
  ev_timer_again (loop, &timer_watcher);

  ev_io_init (&server_watcher, handle_accept, s, EV_READ);
  ev_io_start (loop, &server_watcher);

  if (pthread_create (&io_thread, NULL, node, loop) != 0) {
    cerr << "failed to initialize node (pthread_create)" << endl;
    abort();
  }
}


Process::Process()
{
  static volatile bool initializing = true;
  /* Confirm everything is initialized. */
  if (!initialized) {
    if (__sync_bool_compare_and_swap(&initialized, false, true)) {
      initialize();
      initializing = false;
    }
  }

  while (initializing);

  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  current = NULL;

  generation = 0;

  /* Initialize the PID associated with the process. */
  pid.pipe = __sync_add_and_fetch(&global_pipe, 1);
  pid.ip = ip;
  pid.port = port;
}


void Process::enqueue (struct msg &msg)
{
  pthread_mutex_lock(&mutex);
  {
    //cout << "Process::enqueue for " << pid
    //<< " with state" << state << endl;
    if (state != EXITED) {
      msgs.push(&msg);

      if (state == RECEIVING) {
	state = READY;
	ProcessManager::instance()->enqueue(*this);
      }
    }
  }
  pthread_mutex_unlock(&mutex);
}


struct msg * Process::dequeue()
{
  struct msg *msg = NULL;

  pthread_mutex_lock(&mutex);
  {
    assert (state == RUNNING);
    if (!msgs.empty()) {
      msg = msgs.front();
      msgs.pop();
    }
  }
  pthread_mutex_unlock(&mutex);

  return msg;
}



PID Process::self()
{
  return pid;
}


PID Process::from()
{
  PID pid = { 0, 0, 0 };
  return current != NULL ? current->from : pid;
}


void Process::send(const PID &to, MSGID id, const pair<const char *, size_t> &body)
{
  /* Disallow sending messages using an internal id. */
  if (id < PROCESS_MSGID) {
    return;
  }

  const char *data = body.first;
  size_t len = body.second;

  /* Allocate/Initialize outgoing message. */
  struct msg *msg = (struct msg *) malloc(sizeof(struct msg) + len);

  msg->from.pipe = pid.pipe;
  msg->from.ip = pid.ip;
  msg->from.port = pid.port;
  msg->to.pipe = to.pipe;
  msg->to.ip = to.ip;
  msg->to.port = to.port;
  msg->id = id;
  msg->len = len;

  memcpy((char *) msg + sizeof(struct msg), data, len);

//   cout << endl;
//   cout << "msg.from.pipe: " << msg->from.pipe << endl;
//   cout << "msg.from.ip: " << msg->from.ip << endl;
//   cout << "msg.from.port: " << msg->from.port << endl;
//   cout << "msg.to.pipe: " << msg->to.pipe << endl;
//   cout << "msg.to.ip: " << msg->to.ip << endl;
//   cout << "msg.to.port: " << msg->to.port << endl;
//   cout << "msg.id: " << msg->id << endl;
//   cout << "msg.len: " << msg->len << endl;

  if (to.ip == ip && to.port == port)
    /* Local message. */
    ProcessManager::instance()->deliver(*msg);
  else
    /* Remote message. */
    LinkManager::instance()->send(msg);
}


MSGID Process::receive(time_t secs)
{
  /* Free current message. */
  if (current)
    free(current);

  /* Check if there is a message queued. */
  if ((current = dequeue()) != NULL)
    return current->id;

  if (pthread_self() == proc_thread) {
    /* Avoid blocking if negative seconds. */
    if (secs >= 0)
      ProcessManager::instance()->receive(*this, secs);
    
    /* Check for a message (otherwise we timed out). */
    if ((current = dequeue()) == NULL)
      goto timeout;

  } else {
    /* Do a blocking (spinning) receive if on "outside" thread. */
    /* TODO(benh): Handle timeout. */
    do {
      pthread_mutex_lock(&mutex);
      {
	if (state == TIMEDOUT) {
	  state = RUNNING;
	  pthread_mutex_unlock(&mutex);
	  goto timeout;
	}
	assert(state == RUNNING);
	current = dequeue();
	usleep(50000); // 50000 == ~RTT 
      }
      pthread_mutex_unlock(&mutex);
    } while (current == NULL);
  }

  assert (current != NULL);

  return current->id;

 timeout:
  current = (struct msg *) malloc(sizeof(struct msg));
  current->from.pipe = 0;
  current->from.ip = 0;
  current->from.port = 0;
  current->to.pipe = pid.pipe;
  current->to.ip = pid.ip;
  current->to.port = pid.port;
  current->id = PROCESS_TIMEOUT;
  current->len = 0;

  return current->id;
}


pair<const char *, size_t> Process::body ()
{
  if (current && current->len > 0)
    return make_pair((char *) current + sizeof(struct msg), current->len);
  else
    return make_pair((char *) NULL, 0);
}


void Process::pause (time_t secs)
{
  if (pthread_self() == proc_thread) {
    ProcessManager::instance()->pause(*this, secs);
  } else {
    sleep(secs);
  }
}


PID Process::link(const PID &to)
{
  LinkManager::instance()->link(this, to);
  return to;
}


void Process::await (int fd, int op)
{
  ProcessManager::instance()->await(this, fd, op);
}


bool Process::ready (int fd, int op)
{
  fd_set rdset;
  fd_set wrset;

  FD_ZERO(&rdset);
  FD_ZERO(&wrset);

  if (op & RDWR) {
    FD_SET(fd, &rdset);
    FD_SET(fd, &wrset);
  } else if (op & RDONLY) {
    FD_SET(fd, &rdset);
  } else if (op & WRONLY) {
    FD_SET(fd, &wrset);
  }

  struct timeval timeout;
  memset(&timeout, 0, sizeof(timeout));

  select(fd+1, &rdset, &wrset, NULL, &timeout);

  return FD_ISSET(fd, &rdset) || FD_ISSET(fd, &wrset);
}


PID Process::spawn(Process *process)
{
  if (process != NULL) {
    ProcessManager::instance()->spawn(process);
    return process->pid;
  } else {
    PID pid = { 0, 0, 0 };
    return pid;
  }
}


void Process::wait(const PID &pid)
{
  ProcessManager::instance()->wait(pid);
}
