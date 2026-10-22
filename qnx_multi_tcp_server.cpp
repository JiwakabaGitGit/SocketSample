/**
 * @file    qnx_multi_tcp_server.cpp
 * @brief   QNX 6.x 向けマルチクライアント TCP サーバ（実装部）
 *
 * @details
 * 宣言・API 契約は qnx_multi_tcp_server.h を参照。ここには「どう実装しているか／なぜそうしているか」を書く。
 *
 * @par 資源解放の規則（本実装の最重要ポイント）
 *  クライアントの fd の close と ClientContext の delete は、
 *  「recv/send 両スレッドが終了し、join された後に、reap() が 1 回だけ」行う。
 *  recv/send スレッド自身は決して close / delete しない。これにより、
 *   - 他方のスレッドがまだ使っているメモリを解放する use-after-free、
 *   - fd 番号が再利用された後に古いスレッドが別クライアントの fd を触る事故、
 *   - 待機中の条件変数を destroy する未定義動作
 *  を構造的に防いでいる。
 *
 * @par ロック順序（逆順に取らないこと。デッドロック防止）
 *  clients_mutex_ → ClientContext 内部の mutex
 *  - recv_mutex_ は他のロックと同時に保持しない。
 *  - recv/send スレッドは clients_mutex_ を取らない（自分の ClientContext 内部の mutex のみ）。
 *
 * @par 停止要求の検出方法
 *  無期限ブロックの recv()/accept() は、別スレッドからの close/shutdown で確実に解除される保証が
 *  QNX 6 では無い。そこで、各スレッドは短いタイムアウト（POLL_MS）付きの select() で待ち、
 *  タイムアウトのたびに停止要求（stopping_ / active）を確認する。停止に最大 POLL_MS 程度かかる代わりに、
 *  プラットフォーム依存の挙動に頼らず確実に止まる。
 */

#include "qnx_multi_tcp_server.h"

#include <iostream>
#include <sstream>

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/**
 * @def QNX_MTS_SEND_FLAGS
 * @brief send() に渡すフラグ。
 * @details
 * 切断済みソケットへ send() すると SIGPIPE でプロセスが落ちる。
 * MSG_NOSIGNAL が使える環境ではそれを指定してシグナル自体を抑止する。
 * 使えない環境（古い QNX 等）では 0 とし、代わりに start() で signal(SIGPIPE, SIG_IGN) を設定して同じ効果を得る。
 */
#ifdef MSG_NOSIGNAL
#define QNX_MTS_SEND_FLAGS MSG_NOSIGNAL
#else
#define QNX_MTS_SEND_FLAGS 0
#endif

// ======================================================================
// このファイル内だけで使う定数・ヘルパ（外部に公開しない）
// ======================================================================
namespace {

/**
 * @brief 内部定数。C++03 では定数の置き場に enum か const 変数を使う。
 */
enum {
    POLL_MS        = 200,          ///< select のタイムアウト[ms]。停止フラグ／active の確認周期を兼ねる（停止に最大この時間かかる）
    MAX_LINE       = 64 * 1024,    ///< 1 メッセージの最大長[byte]。区切りが来ないまま超えたら切断（メモリ枯渇・異常クライアント対策）
    SEND_STALL_MS  = 10000,        ///< 送信が全く進まない状態がこの時間続いたら切断[ms]（相手が受信しなくなった場合の対策）
    STACK_SIZE     = 128 * 1024    ///< 各スレッドのスタックサイズ[byte]。クライアント数 × 2 本分のメモリを抑える
};

/**
 * @brief 整数を文字列にする。
 * @details std::to_string は C++11 なので、C++03 互換のため ostringstream で代用。
 */
std::string to_str(long v) {
    std::ostringstream oss;
    oss << v;
    return oss.str();
}

/**
 * @brief fd を非ブロッキングにする。
 * @details
 * select() で「準備完了」を確認してから recv/send/accept するが、稀に準備完了後に状態が変わってブロックしうる。
 * 非ブロッキングにして、スレッドが停止要求に反応できなくなる事態を避ける。
 * @retval true 成功
 */
bool set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

/**
 * @brief fd の読み出し／書き込み可能を最大 timeout_ms 待つ（select のラッパ）。
 *
 * @param fd         監視する fd
 * @param for_write  true なら書き込み可能、false なら読み出し可能を待つ
 * @param timeout_ms タイムアウト[ms]
 * @retval  1 準備完了
 * @retval  0 タイムアウト、または EINTR（シグナルで中断。呼び出し側はループで再試行する）
 * @retval -1 エラー（fd が不正、FD_SETSIZE 以上など）
 *
 * @details
 * EINTR を「エラー」ではなく「0（再試行）」として返すのは、Ctrl+C などのシグナルで
 * 不要な切断が起きないようにするため。fd_set は FD_SETSIZE 未満の fd しか扱えないので範囲を確認する。
 */
int wait_fd(int fd, bool for_write, int timeout_ms) {
    if (fd < 0 || fd >= FD_SETSIZE) return -1;
    fd_set set;
    FD_ZERO(&set);
    FD_SET(fd, &set);
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    int r = select(fd + 1,
                   for_write ? NULL : &set,
                   for_write ? &set : NULL,
                   NULL, &tv);
    if (r < 0) return (errno == EINTR) ? 0 : -1;
    return (r > 0) ? 1 : 0;
}

/**
 * @brief errno が「やり直せば良い一時的な状態」かを判定する。
 * @retval true EINTR（シグナル割り込み）/ EAGAIN / EWOULDBLOCK（非ブロッキングで今は無理）
 * @note これらを「切断・エラー」と誤判定すると、不要な切断が起きる。
 */
bool is_retry_errno(int e) {
    return e == EINTR || e == EAGAIN || e == EWOULDBLOCK;
}

} // namespace

// ======================================================================
// RecvPacket
// ======================================================================

bool RecvPacket::starts_with(const std::string& prefix) const {
    return data_.size() >= prefix.size() &&
           data_.compare(0, prefix.size(), prefix) == 0;
}

/**
 * @details
 * find_first_not_of / find_last_not_of で「最初と最後の非空白」の位置を求め、その範囲だけ切り出す。
 * 手入力（telnet 等）のクライアントでは末尾に余分な空白が付くことがあるため、コマンド照合の前に使うと判定が安定する。
 */
std::string RecvPacket::trimmed() const {
    static const char* const WS = " \t\r\n";
    std::string::size_type b = data_.find_first_not_of(WS);
    if (b == std::string::npos) return std::string();
    std::string::size_type e = data_.find_last_not_of(WS);
    return data_.substr(b, e - b + 1);
}

/**
 * @details
 * find() で次の区切り位置を探して切り出す処理を、区切りが見つからなくなるまで繰り返す。
 * 最後のトークン（最後の区切りより後ろ）も忘れず追加するため、pos == npos のときも 1 回トークンを作ってから抜ける。
 * 先頭・末尾・連続する区切りは空トークンになる（skip_empty=true で除外できる）。
 */
std::vector<std::string> RecvPacket::split(char delim, bool skip_empty) const {
    std::vector<std::string> out;
    std::string::size_type start = 0;
    for (;;) {
        std::string::size_type pos = data_.find(delim, start);
        std::string tok = (pos == std::string::npos)
                              ? data_.substr(start)
                              : data_.substr(start, pos - start);
        if (!(skip_empty && tok.empty())) out.push_back(tok);
        if (pos == std::string::npos) break;
        start = pos + 1;
    }
    return out;
}

/**
 * @details
 * strtol の戻り値だけでは不正入力を見分けられないため、次を全て確認する。
 *  - errno（ERANGE: long の範囲外）
 *  - end == 先頭（1 文字も数字を読めなかった。"abc"）
 *  - *end != '\0'（末尾に未変換の文字が残った。"12abc"）
 *  - long 値が int の範囲内か（long が 64bit の環境で int を超える値を弾く）
 * 不正な入力を黙って 0 として扱うことを避け、失敗時は out を変更しない。
 */
bool RecvPacket::to_int(int& out) const {
    std::string s = trimmed();
    if (s.empty()) return false;
    errno = 0;
    char* end = NULL;
    long v = strtol(s.c_str(), &end, 10);
    if (errno != 0 || end == s.c_str() || *end != '\0') return false;
    if (v < INT_MIN || v > INT_MAX) return false;
    out = static_cast<int>(v);
    return true;
}

std::string RecvPacket::to_string() const {
    std::ostringstream oss;
    oss << "Client #" << client_id_ << ": " << data_;
    return oss.str();
}

/**
 * @note 自由関数（非インライン）。ヘッダでは宣言のみ行い、定義はこの 1 か所に置く（多重定義を避ける）。
 */
std::ostream& operator<<(std::ostream& os, const RecvPacket& p) {
    return os << p.to_string();
}

// ======================================================================
// QnxMultiTcpServer::ClientContext
// ======================================================================

/**
 * @class QnxMultiTcpServer::ClientContext
 * @brief 1 クライアントぶんの状態と、その状態に対するスレッドセーフな操作をまとめたクラス。
 *
 * @details
 * 設計の意図:
 *  - 状態（active フラグ、終了フラグ、送信キュー、mutex、条件変数）を private に閉じ込め、
 *    「ロックを取ってから触る」規則を、このクラスのメソッド内だけで完結させる。
 *    サーバ本体の側は mutex を直接触らず、メソッドを呼ぶだけで済む（ロックの取り忘れ・順序ミスを防ぐ）。
 *  - 生成後に変わらない値（server / id / fd / peer）は const アクセサで公開し、ロック無しで読めるようにする。
 *  - 定義を .cpp に置くことで、ヘッダの利用者に内部構造を見せず、変更時の再コンパイル範囲も小さくする。
 *
 * 所有権: accept スレッド（register_client）が生成し、reap() が両スレッド join 後に delete する。
 * recv/send スレッドはポインタを借りて使うだけで、解放しない。
 *
 * @par 状態遷移
 *  active=true で始まり、停止要求（deactivate）・recv 終了（finish_recv）・send 終了（finish_send）の
 *  いずれかで active=false になる（一方向のみ。true に戻らない）。
 *  recv_done と send_done の両方が true になった時点で、reap() が回収できる（is_finished）。
 */
class QnxMultiTcpServer::ClientContext {
public:
    /**
     * @brief コンストラクタ。同期オブジェクトを初期化する。
     * @param server 所属サーバ（スレッドエントリから本体のメンバ関数を呼ぶため）
     * @param id     クライアント ID
     * @param fd     接続ソケット（所有権は reap() / 登録失敗経路が close_socket() で閉じる形で管理する）
     * @param peer   接続元 "IP:port"（ログ用）
     */
    ClientContext(QnxMultiTcpServer* server, int id, int fd, const std::string& peer)
        : server_(server), id_(id), fd_(fd), peer_(peer),
          active_(true), recv_done_(false), send_done_(false) {
        pthread_mutex_init(&mutex_, NULL);
        pthread_cond_init(&cv_, NULL);
    }

    /**
     * @brief デストラクタ。同期オブジェクトを破棄する。
     * @note fd の close はここでは行わない（reap() が join 後に close_socket() で明示的に行う）。
     *       待機中スレッドがいる状態で呼ばないこと（reap が join 後に delete するので満たされる）。
     */
    ~ClientContext() {
        pthread_cond_destroy(&cv_);
        pthread_mutex_destroy(&mutex_);
    }

    // ---- 生成後に変わらない値（ロック不要）----

    /// @brief 所属サーバを返す。
    QnxMultiTcpServer* server() const { return server_; }
    /// @brief クライアント ID を返す。
    int id() const { return id_; }
    /// @brief 接続ソケットの fd を返す。
    int fd() const { return fd_; }
    /// @brief 接続元 "IP:port" を返す。
    const std::string& peer() const { return peer_; }

    /**
     * @brief 受信スレッドのハンドルへの参照を返す（pthread_create / pthread_join 用）。
     * @note 書き込みは register_client()（スレッド生成時）のみ。以後は変わらないのでロック不要。
     */
    pthread_t& recv_thread() { return recv_thread_; }

    /// @brief 送信スレッドのハンドルへの参照を返す（recv_thread() と同様）。
    pthread_t& send_thread() { return send_thread_; }

    // ---- 状態の読み書き（内部で mutex_ を取る）----

    /// @brief まだ有効（両スレッドが動いてよい状態）か。
    bool is_active() {
        pthread_mutex_lock(&mutex_);
        bool a = active_;
        pthread_mutex_unlock(&mutex_);
        return a;
    }

    /**
     * @brief 停止を要求する（active=false にして、待機中の send スレッドを起こす）。
     * @details stop() や、スレッド生成失敗時の後始末から呼ぶ。fd はここでは閉じない。
     */
    void deactivate() {
        pthread_mutex_lock(&mutex_);
        active_ = false;
        pthread_cond_broadcast(&cv_);
        pthread_mutex_unlock(&mutex_);
    }

    /**
     * @brief recv スレッドの終了を記録する（active=false・recv_done=true にして send スレッドを起こす）。
     * @details close / delete は行わない（send スレッドがまだ使っているため）。
     */
    void finish_recv() {
        pthread_mutex_lock(&mutex_);
        active_ = false;
        recv_done_ = true;
        pthread_cond_broadcast(&cv_);
        pthread_mutex_unlock(&mutex_);
    }

    /// @brief send スレッドの終了を記録する（active=false・send_done=true）。
    void finish_send() {
        pthread_mutex_lock(&mutex_);
        active_ = false;
        send_done_ = true;
        pthread_cond_broadcast(&cv_);
        pthread_mutex_unlock(&mutex_);
    }

    /// @brief 両スレッドが終了済みか（true なら reap() で回収できる）。
    bool is_finished() {
        pthread_mutex_lock(&mutex_);
        bool done = recv_done_ && send_done_;
        pthread_mutex_unlock(&mutex_);
        return done;
    }

    // ---- 送信キュー ----

    /**
     * @brief 送信キューへデータを積み、send スレッドを起こす。
     * @param data      送信データ
     * @param max_queue 送信キューの上限（メッセージ数）
     * @retval true  積めた
     * @retval false 非 active、またはキューが上限に達している
     * @details 呼び出し側が clients_mutex_ を保持している前提（ロック順序: clients_mutex_ → mutex_）。
     */
    bool enqueue_send(const std::string& data, size_t max_queue) {
        bool ok = false;
        pthread_mutex_lock(&mutex_);
        if (active_ && send_queue_.size() < max_queue) {
            send_queue_.push(data);
            pthread_cond_signal(&cv_);
            ok = true;
        }
        pthread_mutex_unlock(&mutex_);
        return ok;
    }

    /**
     * @brief 送信すべきデータが来るまで待ち、1 件取り出す（send スレッド専用）。
     * @param[out] out 取り出したデータ
     * @retval true  取り出せた
     * @retval false 非 active になった（呼び出し側は送信ループを終了する）
     * @details
     * 待機は条件変数で行うため、データが無い間は CPU を消費しない。
     * 起こされる要因は「キューへの追加」と「active=false」の 2 つ。
     * 待機条件を while で再評価するのは、擬似的な起床（spurious wakeup）に対応するため。
     * 取り出したら mutex を解放して戻る。実際の送信は呼び出し側がロック外で行うので、
     * 送信が遅くても enqueue_send()（＝async_send）が詰まらない。
     */
    bool wait_pop_send(std::string& out) {
        pthread_mutex_lock(&mutex_);
        while (active_ && send_queue_.empty()) {
            pthread_cond_wait(&cv_, &mutex_);
        }
        if (!active_) {
            pthread_mutex_unlock(&mutex_);
            return false;
        }
        out = send_queue_.front();
        send_queue_.pop();
        pthread_mutex_unlock(&mutex_);
        return true;
    }

    // ---- ソケット操作 ----

    /**
     * @brief 通信だけを止める（fd は閉じない）。
     * @details 相手へ FIN を送り、相手側スレッドの select/send/recv を速やかにエラー終了させる効果がある。
     */
    void shutdown_socket() { shutdown(fd_, SHUT_RDWR); }

    /**
     * @brief fd を閉じる。
     * @warning 両スレッドの終了（join）後にのみ呼ぶこと（reap() と、スレッド生成失敗時の後始末のみ）。
     *          スレッドが生きている間に閉じると、fd 番号の再利用で別クライアントの fd を触る事故が起こる。
     */
    void close_socket() { close(fd_); }

private:
    /// コピー禁止（mutex を含むため）。C++03 流に宣言のみで実装しない。
    ClientContext(const ClientContext&);
    ClientContext& operator=(const ClientContext&);

    QnxMultiTcpServer* server_;     ///< 所属サーバ
    int id_;                        ///< クライアント ID
    int fd_;                        ///< 接続ソケット
    std::string peer_;              ///< 接続元 "IP:port"
    pthread_t recv_thread_;         ///< 受信スレッド（reap で join）
    pthread_t send_thread_;         ///< 送信スレッド（reap で join）

    pthread_mutex_t mutex_;         ///< 以下の可変メンバを保護するミューテックス
    pthread_cond_t cv_;             ///< 送信キューへの追加／停止を send スレッドへ知らせる条件変数
    std::queue<std::string> send_queue_; ///< 送信待ちデータ（mutex_ で保護）
    bool active_;                   ///< false になったら両スレッドは終了へ向かう（mutex_ で保護）
    bool recv_done_;                ///< recv スレッドが終了処理まで済ませた（mutex_ で保護）
    bool send_done_;                ///< send スレッドが終了処理まで済ませた（mutex_ で保護）
};

// ======================================================================
// QnxMultiTcpServer: 構築・破棄
// ======================================================================

QnxMultiTcpServer::QnxMultiTcpServer(int port,
                                     size_t max_clients,
                                     size_t max_recv_queue,
                                     size_t max_send_queue)
    : port_(port),
      max_clients_(max_clients),
      max_recv_queue_(max_recv_queue),
      max_send_queue_(max_send_queue),
      listen_fd_(-1),
      started_(false),
      stopping_(false),
      next_client_id_(1) {
    pthread_mutex_init(&state_mutex_, NULL);
    pthread_mutex_init(&clients_mutex_, NULL);
    pthread_mutex_init(&recv_mutex_, NULL);
    pthread_cond_init(&recv_cv_, NULL);
    pthread_mutex_init(&log_mutex_, NULL);
}

/**
 * @details
 * 「スレッドが生きている間に mutex / cond を destroy しない」ことが重要なので、
 * 必ず stop()（＝全スレッド join 済み）を先に呼ぶ。stop() は多重呼び出ししても安全。
 */
QnxMultiTcpServer::~QnxMultiTcpServer() {
    stop();
    pthread_cond_destroy(&recv_cv_);
    pthread_mutex_destroy(&recv_mutex_);
    pthread_mutex_destroy(&clients_mutex_);
    pthread_mutex_destroy(&state_mutex_);
    pthread_mutex_destroy(&log_mutex_);
}

// ======================================================================
// QnxMultiTcpServer: 開始・停止
// ======================================================================

/**
 * @details
 * socket() / bind() / listen() を accept スレッドの中ではなくここで「同期的に」行う。
 * 理由: 別スレッドで行うと bind 失敗（ポート使用中など）が呼び出し側に伝わらず、
 * start() が true を返したのにサーバが動いていない、という状態になるため。
 *
 * 主な処理:
 *  -# SIGPIPE を無視に設定（切断済みソケットへの send でプロセスが落ちるのを防ぐ）。
 *  -# socket 作成。select() で扱える fd 番号（FD_SETSIZE 未満）かを確認。
 *  -# SO_REUSEADDR を設定（再起動直後の TIME_WAIT による bind 失敗を避ける）。
 *  -# bind → listen。
 *  -# 待ち受けソケットを非ブロッキングに設定
 *     （select() が「読める」と返した後に接続が消えても accept() が固まらないように）。
 *  -# accept スレッドを起動。
 *
 * どの段階で失敗しても、それまでに作った資源（listen_fd_）は close_listen() で戻してから false を返す。
 */
bool QnxMultiTcpServer::start() {
    if (started_) return false;

    signal(SIGPIPE, SIG_IGN);

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        log_line(std::string("[Server] socket() failed: ") + strerror(errno));
        listen_fd_ = -1;
        return false;
    }
    if (listen_fd_ >= FD_SETSIZE) {
        log_line("[Server] listen fd exceeds FD_SETSIZE");
        close_listen();
        return false;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<unsigned short>(port_));
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        log_line(std::string("[Server] bind() failed: ") + strerror(errno));
        close_listen();
        return false;
    }
    if (listen(listen_fd_, 16) < 0) {
        log_line(std::string("[Server] listen() failed: ") + strerror(errno));
        close_listen();
        return false;
    }
    if (!set_nonblocking(listen_fd_)) {
        log_line("[Server] failed to set non-blocking on listen fd");
        close_listen();
        return false;
    }

    set_stopping(false);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);
    int rc = pthread_create(&accept_tid_, &attr, accept_entry, this);
    pthread_attr_destroy(&attr);
    if (rc != 0) {
        log_line("[Server] failed to create accept thread");
        close_listen();
        return false;
    }

    started_ = true;
    return true;
}

/**
 * @details
 * 停止手順の順序には意味がある:
 *  -# 停止フラグを立て、accept スレッドを join する。
 *     → 以後、新しいクライアントは増えない（先にクライアントを止めると、その最中に新規接続が混入しうる）。
 *  -# 待ち受けソケットを閉じる。
 *  -# 全クライアントを active=false にし、条件変数を broadcast し、shutdown() する。
 *     → 待機中の send スレッド（cond_wait）と recv スレッド（select/recv）を起こす。
 *        この時点では fd を close しない（スレッドがまだ使っている可能性があるため）。
 *  -# reap(true) で全クライアントの両スレッドを join し、close / delete する。
 *  -# wait_pop_recv() で待機中のスレッドを起こす（main 以外が待っていても抜けられるように）。
 */
void QnxMultiTcpServer::stop() {
    if (!started_) return;

    // 1. accept スレッドを止める（以後、新規クライアントは増えない）
    set_stopping(true);
    pthread_join(accept_tid_, NULL);
    close_listen();

    // 2. 全クライアントに停止を通知（fd はまだ close しない）
    pthread_mutex_lock(&clients_mutex_);
    for (ClientMap::iterator it = clients_.begin(); it != clients_.end(); ++it) {
        ClientContext* c = it->second;
        c->deactivate();
        c->shutdown_socket();
    }
    pthread_mutex_unlock(&clients_mutex_);

    // 3. 全員 join → close → delete
    reap(true);

    // 4. wait_pop_recv() で待っているスレッドを起こす
    pthread_mutex_lock(&recv_mutex_);
    pthread_cond_broadcast(&recv_cv_);
    pthread_mutex_unlock(&recv_mutex_);

    started_ = false;
}

// ======================================================================
// QnxMultiTcpServer: 送信 API
// ======================================================================

/**
 * @details
 * clients_mutex_ を保持している間は、reap() が clients_ から erase（＝後で delete）できない。
 * そのため find() で得た ClientContext* は enqueue_send() の間、確実に有効。
 * 実際の送信は行わずキューに積むだけなので、呼び出しはネットワーク状況に関わらず即座に戻る。
 */
bool QnxMultiTcpServer::async_send(int client_id, const std::string& data) {
    bool ok = false;
    pthread_mutex_lock(&clients_mutex_);
    ClientMap::iterator it = clients_.find(client_id);
    if (it != clients_.end()) {
        ok = it->second->enqueue_send(data, max_send_queue_);
    }
    pthread_mutex_unlock(&clients_mutex_);
    return ok;
}

size_t QnxMultiTcpServer::async_send_all(const std::string& data) {
    size_t count = 0;
    pthread_mutex_lock(&clients_mutex_);
    for (ClientMap::iterator it = clients_.begin(); it != clients_.end(); ++it) {
        if (it->second->enqueue_send(data, max_send_queue_)) ++count;
    }
    pthread_mutex_unlock(&clients_mutex_);
    return count;
}

// ======================================================================
// QnxMultiTcpServer: 受信 API
// ======================================================================

bool QnxMultiTcpServer::try_pop_recv(RecvPacket& out_packet) {
    pthread_mutex_lock(&recv_mutex_);
    bool ok = pop_recv_locked(out_packet);
    pthread_mutex_unlock(&recv_mutex_);
    return ok;
}

/**
 * @details
 * 条件変数で待つため、データが届いた瞬間に起床する。
 * usleep によるポーリングと違い、ポーリング周期ぶんの遅延が乗らず、空き時間の CPU 負荷も小さい。
 * pthread_cond_timedwait の期限は「絶対時刻（CLOCK_REALTIME）」で指定する必要があるため、
 * 現在時刻に timeout_ms を加算して期限を作っている（tv_nsec の繰り上がりも処理する）。
 * 擬似的な起床（spurious wakeup）に備え、キューが空である間は while で待ち直す。
 */
bool QnxMultiTcpServer::wait_pop_recv(RecvPacket& out_packet, int timeout_ms) {
    pthread_mutex_lock(&recv_mutex_);
    if (recv_queue_.empty() && timeout_ms > 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += static_cast<long>(timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }
        while (recv_queue_.empty()) {
            if (pthread_cond_timedwait(&recv_cv_, &recv_mutex_, &ts) != 0) break; // ETIMEDOUT 等
        }
    }
    bool ok = pop_recv_locked(out_packet);
    pthread_mutex_unlock(&recv_mutex_);
    return ok;
}

size_t QnxMultiTcpServer::client_count() {
    pthread_mutex_lock(&clients_mutex_);
    size_t n = clients_.size();
    pthread_mutex_unlock(&clients_mutex_);
    return n;
}

/**
 * @details
 * 複数スレッドが同時に std::cout へ書くと出力が混ざるため、専用 mutex で直列化する。
 */
void QnxMultiTcpServer::log_line(const std::string& msg) {
    pthread_mutex_lock(&log_mutex_);
    std::cout << msg << std::endl;
    pthread_mutex_unlock(&log_mutex_);
}

// ======================================================================
// QnxMultiTcpServer: 内部ユーティリティ
// ======================================================================

void QnxMultiTcpServer::close_listen() {
    if (listen_fd_ != -1) {
        close(listen_fd_);
        listen_fd_ = -1;
    }
}

/**
 * @details 停止フラグは volatile ではなく mutex で保護する（volatile はスレッド間の同期・可視性を保証しないため）。
 */
bool QnxMultiTcpServer::is_stopping() {
    pthread_mutex_lock(&state_mutex_);
    bool s = stopping_;
    pthread_mutex_unlock(&state_mutex_);
    return s;
}

void QnxMultiTcpServer::set_stopping(bool v) {
    pthread_mutex_lock(&state_mutex_);
    stopping_ = v;
    pthread_mutex_unlock(&state_mutex_);
}

bool QnxMultiTcpServer::pop_recv_locked(RecvPacket& out) {
    if (recv_queue_.empty()) return false;
    out = recv_queue_.front();
    recv_queue_.pop();
    return true;
}

// ======================================================================
// QnxMultiTcpServer: スレッドエントリ
// ======================================================================

void* QnxMultiTcpServer::accept_entry(void* arg) {
    static_cast<QnxMultiTcpServer*>(arg)->accept_loop();
    return NULL;
}

/**
 * @note 元実装のように new した引数構造体を渡して delete する方式は、
 *       生成失敗時のリークや二重管理の元になるため、ClientContext* を直接渡す。
 */
void* QnxMultiTcpServer::recv_entry(void* arg) {
    ClientContext* c = static_cast<ClientContext*>(arg);
    c->server()->recv_loop(c);
    return NULL;
}

void* QnxMultiTcpServer::send_entry(void* arg) {
    ClientContext* c = static_cast<ClientContext*>(arg);
    c->server()->send_loop(c);
    return NULL;
}

// ======================================================================
// QnxMultiTcpServer: accept
// ======================================================================

/**
 * @details
 * ループ 1 周ごとに（最大 POLL_MS 間隔で）次を行う:
 *  -# reap(false): 両スレッドが終了したクライアントの回収。
 *     専用の回収スレッドを増やさず、accept スレッドが兼任することでスレッド数を節約している。
 *  -# select() で新規接続を待つ（タイムアウト付き → 停止要求にすぐ反応できる）。
 *  -# accept() し、register_client() に渡す。
 *
 * accept() の失敗時の扱い:
 *  - EINTR / EAGAIN / ECONNABORTED は一時的なので即リトライ。
 *  - それ以外（EMFILE: fd 枯渇など）は条件が続く限り失敗し続けるため、
 *    100ms 待ってからリトライする（CPU 100% のビジーループ回避）。
 */
void QnxMultiTcpServer::accept_loop() {
    while (!is_stopping()) {
        reap(false);   // 切断済みクライアントの回収

        int r = wait_fd(listen_fd_, false, POLL_MS);
        if (r < 0) {
            log_line("[Server] select() on listen socket failed");
            usleep(100000);
            continue;
        }
        if (r == 0) continue;

        struct sockaddr_in c_addr;
        socklen_t len = sizeof(c_addr);
        int fd = accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&c_addr), &len);
        if (fd < 0) {
            if (is_retry_errno(errno) || errno == ECONNABORTED) continue;
            // EMFILE / ENFILE / ENOBUFS 等: ビジーループを避ける
            log_line(std::string("[Server] accept() failed: ") + strerror(errno));
            usleep(100000);
            continue;
        }

        register_client(fd, c_addr);   // fd の所有権はここで移る（失敗時は内部で close）
    }
}

/**
 * @details
 * 失敗経路ごとに「それまでに確保した資源だけを」確実に戻すのがポイント。
 *  - 上限超過／fd 設定失敗 → fd を close するだけ。
 *  - recv スレッド生成失敗 → fd を close、ClientContext を delete。
 *  - send スレッド生成失敗 → 既に動いている recv スレッドを止めて join してから close / delete。
 *
 * clients_ への登録は「両スレッド起動に成功した後」に行う。
 * 登録前にスレッドが先に終了しても、done フラグが立ったまま登録されるため、次回の reap() で正しく回収される。
 */
bool QnxMultiTcpServer::register_client(int fd, const struct sockaddr_in& addr) {
    if (fd >= FD_SETSIZE || !set_nonblocking(fd)) {
        log_line("[Server] rejected connection (fd setup failed)");
        close(fd);
        return false;
    }
    if (client_count() >= max_clients_) {
        log_line("[Server] rejected connection (max clients reached)");
        close(fd);
        return false;
    }

    int on = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));   // 小さなメッセージの遅延（Nagle）を避ける
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));   // 相手が黙って消えた（半開接続）場合の検出に役立つ

    const std::string peer = std::string(inet_ntoa(addr.sin_addr)) + ":" + to_str(ntohs(addr.sin_port));
    ClientContext* c = new ClientContext(this, next_client_id_++, fd, peer);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, STACK_SIZE);

    if (pthread_create(&c->recv_thread(), &attr, recv_entry, c) != 0) {
        pthread_attr_destroy(&attr);
        log_line("[Server] failed to create recv thread");
        c->close_socket();
        delete c;
        return false;
    }
    if (pthread_create(&c->send_thread(), &attr, send_entry, c) != 0) {
        pthread_attr_destroy(&attr);
        log_line("[Server] failed to create send thread");
        // 起動済みの recv スレッドを止めて回収
        c->deactivate();
        c->shutdown_socket();
        pthread_join(c->recv_thread(), NULL);
        c->close_socket();
        delete c;
        return false;
    }
    pthread_attr_destroy(&attr);

    pthread_mutex_lock(&clients_mutex_);
    clients_[c->id()] = c;
    pthread_mutex_unlock(&clients_mutex_);

    log_line("[Server] Client connected. ID: " + to_str(c->id()) + " (" + c->peer() + ")");
    return true;
}

// ======================================================================
// QnxMultiTcpServer: recv
// ======================================================================

/**
 * @details
 * 受信バッファ buf は「このスレッドのローカル変数」。
 * TCP はメッセージ境界を保存しないため、recv() の結果を buf に蓄積し、
 * 区切り（'\n'）が揃った分だけ切り出す（残りは次回の recv に持ち越す）。
 * buf をクライアントごとに持つことで、複数クライアントのデータが混ざらない。
 *
 * 終了条件は 3 つ（いずれも break してループ外の終了処理へ）:
 *  - recv() が 0   : 相手が正常に切断した。
 *  - recv() が負で EINTR/EAGAIN 以外 : 通信エラー。
 *  - extract_lines() が false : 行長超過、または停止要求で push できない。
 *
 * 終了処理では「active=false・recv_done=true にして send スレッドを起こす」ことだけを行う。
 * fd の close や ClientContext の delete は絶対にしない（send スレッドがまだ使っているため）。
 * shutdown() は fd を閉じずに通信だけを止める操作で、相手側へ FIN を送り、
 * 同時に send スレッドの select/send を速やかにエラー終了させる効果がある。
 */
void QnxMultiTcpServer::recv_loop(ClientContext* c) {
    std::string buf;
    char tmp[4096];

    while (c->is_active()) {
        int r = wait_fd(c->fd(), false, POLL_MS);
        if (r < 0) break;
        if (r == 0) continue;

        ssize_t n = recv(c->fd(), tmp, sizeof(tmp), 0);
        if (n == 0) break;                         // 相手が正常切断
        if (n < 0) {
            if (is_retry_errno(errno)) continue;
            break;                                 // エラー
        }

        buf.append(tmp, static_cast<size_t>(n));   // バイナリ(\0)も安全（std::string(tmp) だと \0 で途切れる）
        if (!extract_lines(c, buf)) break;
    }

    // 終了通知のみ。close / delete は reap() が行う。
    c->finish_recv();        // active=false・recv_done=true にして send スレッドを起こす
    c->shutdown_socket();
}

/**
 * @details
 *  - "\r\n" 区切りにも対応するため、行末の '\r' を取り除く。
 *  - 空行は無視する（改行のみの送信などを許容）。
 *  - 区切りが来ないまま MAX_LINE を超えたら異常とみなして false を返す。
 *    これが無いと、改行を送らない相手にメモリを使い尽くされる。
 *  - 切り出した分は buf の先頭から削除し、未完成の残りだけを次回に持ち越す。
 */
bool QnxMultiTcpServer::extract_lines(ClientContext* c, std::string& buf) {
    std::string::size_type pos;
    while ((pos = buf.find('\n')) != std::string::npos) {
        std::string line = buf.substr(0, pos);
        buf.erase(0, pos + 1);
        if (!line.empty() && line[line.size() - 1] == '\r') {
            line.erase(line.size() - 1);
        }
        if (line.empty()) continue;
        if (!push_recv(c, line)) return false;
    }
    if (buf.size() > static_cast<size_t>(MAX_LINE)) {
        log_line("[Server] Client #" + to_str(c->id()) + " exceeded max line length; disconnecting");
        return false;
    }
    return true;
}

/**
 * @details
 * 満杯時にデータを捨てると「黙ってメッセージが消える」ため採用せず、待機を選んでいる。
 * 待機中は recv() を呼ばないので、カーネルの受信バッファ → TCP ウィンドウの順に埋まり、送信側が自然に減速する。
 * 待機中も 10ms ごとに client_active() を確認し、停止要求・切断があれば抜ける（main が止まっていても stop() が固まらない）。
 * ロックは「キュー操作の間だけ」保持し、待機（usleep）中は保持しない。
 */
bool QnxMultiTcpServer::push_recv(ClientContext* c, const std::string& line) {
    for (;;) {
        pthread_mutex_lock(&recv_mutex_);
        if (recv_queue_.size() < max_recv_queue_) {
            recv_queue_.push(RecvPacket(c->id(), line));
            pthread_cond_signal(&recv_cv_);
            pthread_mutex_unlock(&recv_mutex_);
            return true;
        }
        pthread_mutex_unlock(&recv_mutex_);
        if (!c->is_active()) return false;
        usleep(10000);
    }
}

// ======================================================================
// QnxMultiTcpServer: send
// ======================================================================

/**
 * @details
 * 待機・取り出しの詳細（条件変数、spurious wakeup 対策）は ClientContext::wait_pop_send() を参照。
 *
 * 実際の送信（send_all）は mutex を「解放してから」行う（wait_pop_send は取り出したらロックを解放して戻る）。
 * 送信は相手次第で時間がかかるので、保持したままだと async_send() が詰まってしまう。
 *
 * 終了処理は recv_loop と対称で、active=false・send_done=true にするだけ。
 * close / delete は reap() が行う。最後の shutdown() で recv スレッドの select を起こし、
 * 片方が終わればもう片方も速やかに終わるようにしている。
 */
void QnxMultiTcpServer::send_loop(ClientContext* c) {
    for (;;) {
        std::string data;
        if (!c->wait_pop_send(data)) break;   // データ到着まで待機。非 active なら false
        if (!send_all(c, data)) break;        // 送信はロック外で行う
    }

    c->finish_send();
    c->shutdown_socket();   // recv スレッドを速やかに終了させる
}

/**
 * @details
 * send() は要求したバイト数より少なく返すことがある（部分送信）。
 * そのため、送った分だけ off を進めて全量送れるまで繰り返す。
 * EINTR / EAGAIN は再試行する（切断扱いにしない）。
 * 書き込み可能を select() で待ち、タイムアウトのたびに停止要求を確認する。
 * 「全く進まない状態」が SEND_STALL_MS 続いたら、受信しなくなった相手とみなして切断する
 * （相手が詰まったまま送信スレッドが永久に居座るのを防ぐ）。進捗があれば stalled_ms をリセットする。
 */
bool QnxMultiTcpServer::send_all(ClientContext* c, const std::string& data) {
    size_t off = 0;
    int stalled_ms = 0;
    while (off < data.size()) {
        if (!c->is_active()) return false;

        int r = wait_fd(c->fd(), true, POLL_MS);
        if (r < 0) return false;
        if (r == 0) {
            stalled_ms += POLL_MS;
            if (stalled_ms >= SEND_STALL_MS) {
                log_line("[Server] Client #" + to_str(c->id()) + " send stalled; disconnecting");
                return false;
            }
            continue;
        }

        ssize_t n = send(c->fd(), data.data() + off, data.size() - off, QNX_MTS_SEND_FLAGS);
        if (n < 0) {
            if (is_retry_errno(errno)) continue;
            return false;
        }
        off += static_cast<size_t>(n);
        stalled_ms = 0;
    }
    return true;
}

// ======================================================================
// QnxMultiTcpServer: 回収
// ======================================================================

/**
 * @details
 * fd の close と ClientContext の delete を行う「唯一の場所」。
 * all=false は両スレッドが終了済み（recv_done && send_done）のものだけ、
 * all=true は stop() 用で全クライアントを回収する（呼び出し前に active=false 等で停止通知済みであること）。
 *
 * 手順を 2 段階に分けているのがポイント:
 *  -# clients_mutex_ を保持して clients_ から外し、回収リスト dead に移す（短時間のみロック）。
 *  -# ロックを解放してから join / close / delete を行う。
 *     join は相手スレッドの終了待ちで時間がかかりうるため、ロック保持のまま行うと
 *     その間 async_send() など clients_mutex_ を使う処理がすべて止まってしまう。
 *
 * 先に clients_ から外してあるので、以後 async_send() からは見えず、
 * 他スレッドがこのクライアントを参照することは無い（安全に delete できる）。
 *
 * std::map の走査中の erase は、C++03 では erase(it++) の形で行う
 * （erase 後に it を進めると無効なイテレータを触ってしまうため）。
 */
void QnxMultiTcpServer::reap(bool all) {
    std::vector<ClientContext*> dead;

    pthread_mutex_lock(&clients_mutex_);
    ClientMap::iterator it = clients_.begin();
    while (it != clients_.end()) {
        ClientContext* c = it->second;
        bool done = c->is_finished();

        if (done || all) {
            dead.push_back(c);
            clients_.erase(it++);   // C++03 の定石
        } else {
            ++it;
        }
    }
    pthread_mutex_unlock(&clients_mutex_);

    // ロックの外で join / close / delete（ここが唯一の解放箇所）
    for (size_t i = 0; i < dead.size(); ++i) {
        ClientContext* c = dead[i];
        pthread_join(c->recv_thread(), NULL);
        pthread_join(c->send_thread(), NULL);
        c->close_socket();
        log_line("[Server] Client disconnected. ID: " + to_str(c->id()));
        delete c;
    }
}
