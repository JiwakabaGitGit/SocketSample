/**
 * @file    qnx_multi_tcp_server.h
 * @brief   QNX 6.x 向けマルチクライアント TCP サーバ（ヘッダオンリー実装）
 *
 * @details
 * 複数クライアントと同時に接続し、次の 2 点を実現するサーバクラス QnxMultiTcpServer を提供する。
 *  - main（アプリケーション）側から「クライアント ID を指定して」非同期に送信できる。
 *  - 受信データは「どのクライアントから来たか（client_id）」付きのキューで受け取れる。
 *
 * @par 動作環境・方針
 *  - C++03 互換（nullptr / auto / std::to_string / 波括弧初期化は使わない）。
 *    QNX 6 の標準ツールチェーン（gcc 4.x 系）でも C++11 無しでビルドできるようにするため。
 *  - 依存は POSIX threads / BSD sockets / select() のみ。
 *    QNX 6 で利用可否が不確かな poll() は使わず、確実に使える select() を採用している。
 *
 * @par メッセージ仕様
 *  - 受信: '\n' 区切り（"\r\n" も可）を 1 メッセージとして扱う。
 *    RecvPacket::data には区切り文字を含めない。
 *    TCP はバイトストリームであり recv() の 1 回分がメッセージ 1 個とは限らない
 *    （分割・結合が起こる）ため、区切り文字でフレーミングしている。
 *  - 送信: async_send() は渡された文字列を「そのまま」送る。
 *    区切りの '\n' は呼び出し側で付けること。
 *
 * @par スレッド構成
 *  - accept スレッド × 1 : 接続受付と、切断済みクライアントの回収（reap）を担当。
 *  - recv スレッド × N   : クライアントごとに 1 本。受信とメッセージ切り出しを担当。
 *  - send スレッド × N   : クライアントごとに 1 本。送信キューを取り出して送信を担当。
 *  - main スレッド       : start()/stop()、async_send()、wait_pop_recv() を呼ぶ側。
 *
 * @par 資源解放の規則（本実装の最重要ポイント）
 *  クライアントの fd の close と ClientContext の delete は、
 *  「recv/send 両スレッドが終了し、join された後に、reap() が 1 回だけ」行う。
 *  recv/send スレッド自身は決して close / delete しない。
 *  これにより、
 *   - 他方のスレッドがまだ使っているメモリを解放する use-after-free、
 *   - fd 番号が再利用された後に古いスレッドが別クライアントの fd を触る事故、
 *   - 待機中の条件変数を destroy する未定義動作
 *  を構造的に防いでいる。
 *
 * @par ロック順序（逆順に取らないこと。デッドロック防止）
 *  clients_mutex_ → ClientContext::mutex
 *  - recv_mutex_ は他のロックと同時に保持しない。
 *  - recv/send スレッドは clients_mutex_ を取らない（自分の ClientContext::mutex のみ）。
 *
 * @note ソース内のコメントは UTF-8 で記述している。
 *       エディタ／IDE の文字コード設定が UTF-8 であることを確認すること。
 */

#ifndef QNX_MULTI_TCP_SERVER_H
#define QNX_MULTI_TCP_SERVER_H

#include <iostream>
#include <sstream>
#include <string>
#include <queue>
#include <map>
#include <vector>

#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/**
 * @def QNX_MTS_SEND_FLAGS
 * @brief send() に渡すフラグ。
 * @details
 * 切断済みソケットへ send() すると SIGPIPE でプロセスが落ちる。
 * MSG_NOSIGNAL が使える環境ではそれを指定してシグナル自体を抑止する。
 * 使えない環境（古い QNX 等）では 0 とし、代わりに start() で
 * signal(SIGPIPE, SIG_IGN) を設定して同じ効果を得る。
 */
#ifdef MSG_NOSIGNAL
#define QNX_MTS_SEND_FLAGS MSG_NOSIGNAL
#else
#define QNX_MTS_SEND_FLAGS 0   // SIGPIPE は start() で SIG_IGN にする
#endif

/**
 * @struct RecvPacket
 * @brief  受信キューに積まれる 1 メッセージ。「誰から来たか」を識別するための構造体。
 */
struct RecvPacket {
    int client_id;      ///< 送信元クライアントの ID（サーバが接続順に 1 から採番。async_send の宛先指定にそのまま使える）
    std::string data;   ///< メッセージ本体（区切りの '\n' / "\r\n" は含まない）

    /// @brief 空のパケットを作る。pop 用の受け皿として使う。
    RecvPacket() : client_id(0) {}

    /**
     * @brief 値を指定してパケットを作る。
     * @param id 送信元クライアント ID
     * @param d  メッセージ本体
     */
    RecvPacket(int id, const std::string& d) : client_id(id), data(d) {}
};

/**
 * @class QnxMultiTcpServer
 * @brief 複数クライアント対応の非同期 TCP サーバ。
 *
 * @details
 * 使い方の概要:
 * @code
 *   QnxMultiTcpServer server(8080);
 *   if (!server.start()) { ...失敗処理...; }
 *   while (running) {
 *       RecvPacket p;
 *       if (server.wait_pop_recv(p, 100)) {
 *           server.async_send(p.client_id, "reply\n");
 *       }
 *   }
 *   server.stop();
 * @endcode
 *
 * @warning start() / stop() は同一スレッド（通常は main）からのみ呼ぶこと。
 *          async_send / async_send_all / try_pop_recv / wait_pop_recv / log_line は
 *          複数スレッドから呼んでよい。
 */
class QnxMultiTcpServer {
public:
    /**
     * @brief コンストラクタ。ポート番号と各種上限値を設定し、同期オブジェクトを初期化する。
     *
     * @param port            待ち受けポート番号
     * @param max_clients     同時接続数の上限。超過した接続は accept 直後に切断する。
     *                        クライアント 1 つにつき 2 スレッドを消費するため、
     *                        組込み機器の資源（スレッド数・メモリ）に合わせて決めること。
     * @param max_recv_queue  受信キューの上限（メッセージ数）。満杯の間は recv スレッドが待機し、
     *                        結果として TCP の受信ウィンドウが埋まり送信側が自然に減速する
     *                        （バックプレッシャ）。メモリの際限ない増加を防ぐ。
     * @param max_send_queue  クライアントごとの送信キュー上限（メッセージ数）。
     *                        満杯時は async_send() が false を返す。
     *
     * @note explicit: int からの暗黙変換でサーバオブジェクトができるのを防ぐ。
     */
    explicit QnxMultiTcpServer(int port,
                               size_t max_clients = 32,
                               size_t max_recv_queue = 1024,
                               size_t max_send_queue = 256)
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
     * @brief デストラクタ。稼働中なら stop() で全スレッドを止めてから同期オブジェクトを破棄する。
     * @details
     * 「スレッドが生きている間に mutex / cond を destroy しない」ことが重要なので、
     * 必ず stop()（＝全スレッド join 済み）を先に呼ぶ。stop() は多重呼び出ししても安全。
     */
    ~QnxMultiTcpServer() {
        stop();
        pthread_cond_destroy(&recv_cv_);
        pthread_mutex_destroy(&recv_mutex_);
        pthread_mutex_destroy(&clients_mutex_);
        pthread_mutex_destroy(&state_mutex_);
        pthread_mutex_destroy(&log_mutex_);
    }

    /**
     * @brief サーバを開始する（待ち受けソケットを作り、accept スレッドを起動する）。
     *
     * @retval true  開始成功
     * @retval false 開始失敗（既に開始済み、socket/bind/listen/スレッド生成の失敗など）
     *
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
     *     （select() が「読める」と返した後に別の経路で接続が消えても accept() が固まらないように）。
     *  -# accept スレッドを起動。
     *
     * @warning start()/stop() は同一スレッドから呼ぶこと。
     */
    bool start() {
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
     * @brief サーバを停止する。全スレッドを join し、全クライアントの資源を解放してから戻る。
     *
     * @details
     * 複数回呼んでも安全（未開始・停止済みなら何もしない）。
     * 停止手順の順序には意味がある:
     *  -# 停止フラグを立て、accept スレッドを join する。
     *     → 以後、新しいクライアントは増えない。
     *        （先にクライアントを止めると、その最中に新規接続が混入しうる）
     *  -# 待ち受けソケットを閉じる。
     *  -# 全クライアントを active=false にし、条件変数を broadcast し、shutdown() する。
     *     → 待機中の send スレッド（cond_wait）と recv スレッド（select/recv）を起こす。
     *        この時点では fd を close しない（スレッドがまだ使っている可能性があるため）。
     *  -# reap(true) で全クライアントの両スレッドを join し、close / delete する。
     *  -# wait_pop_recv() で待機中のスレッドを起こす（main 以外が待っていても抜けられるように）。
     */
    void stop() {
        if (!started_) return;

        // 1. accept スレッドを止める（以後、新規クライアントは増えない）
        set_stopping(true);
        pthread_join(accept_tid_, NULL);
        close_listen();

        // 2. 全クライアントに停止を通知（fd はまだ close しない）
        pthread_mutex_lock(&clients_mutex_);
        for (ClientMap::iterator it = clients_.begin(); it != clients_.end(); ++it) {
            ClientContext* c = it->second;
            pthread_mutex_lock(&c->mutex);
            c->active = false;
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mutex);
            shutdown(c->fd, SHUT_RDWR);
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

    /**
     * @brief 指定クライアントへ非同期でデータを送る（実際の送信はそのクライアントの send スレッドが行う）。
     *
     * @param client_id 宛先クライアント ID（RecvPacket::client_id で得られる値）
     * @param data      送信データ（そのまま送られる。区切りの '\n' は呼び出し側で付ける）
     *
     * @retval true  送信キューに積めた（相手に届くことの保証ではない）
     * @retval false クライアントが存在しない／切断済み／送信キューが上限に達している
     *
     * @details
     * 呼び出しはキューに積むだけで即座に戻る。ネットワークが詰まっていても main をブロックしない。
     * clients_mutex_ を保持している間は clients_ からの erase（＝delete）が起きないので、
     * 取得した ClientContext* は enqueue_send() の間、確実に有効。
     */
    bool async_send(int client_id, const std::string& data) {
        bool ok = false;
        pthread_mutex_lock(&clients_mutex_);
        ClientMap::iterator it = clients_.find(client_id);
        if (it != clients_.end()) {
            ok = enqueue_send(it->second, data);
        }
        pthread_mutex_unlock(&clients_mutex_);
        return ok;
    }

    /**
     * @brief 接続中の全クライアントへ一斉送信する。
     * @param data 送信データ（そのまま送られる）
     * @return 送信キューに積めたクライアント数
     */
    size_t async_send_all(const std::string& data) {
        size_t count = 0;
        pthread_mutex_lock(&clients_mutex_);
        for (ClientMap::iterator it = clients_.begin(); it != clients_.end(); ++it) {
            if (enqueue_send(it->second, data)) ++count;
        }
        pthread_mutex_unlock(&clients_mutex_);
        return count;
    }

    /**
     * @brief 受信キューから 1 件取り出す（ブロックしない）。
     * @param[out] out_packet 取り出したパケットの格納先
     * @retval true  取り出せた
     * @retval false キューが空
     */
    bool try_pop_recv(RecvPacket& out_packet) {
        pthread_mutex_lock(&recv_mutex_);
        bool ok = pop_recv_locked(out_packet);
        pthread_mutex_unlock(&recv_mutex_);
        return ok;
    }

    /**
     * @brief 受信キューから 1 件取り出す。空なら最大 timeout_ms ミリ秒待つ。
     *
     * @param[out] out_packet 取り出したパケットの格納先
     * @param      timeout_ms 最大待ち時間[ms]。0 以下なら待たずに try_pop_recv() と同じ動作。
     * @retval true  取り出せた
     * @retval false タイムアウト（または stop() による起床で空のまま）
     *
     * @details
     * 条件変数で待つため、データが届いた瞬間に起床する。
     * usleep によるポーリングと違い、最大でポーリング周期ぶんの遅延が乗らず、空き時間の CPU 負荷も小さい。
     * pthread_cond_timedwait の期限は「絶対時刻（CLOCK_REALTIME）」で指定する必要があるため、
     * 現在時刻に timeout_ms を加算して期限を作っている。
     * 擬似的な起床（spurious wakeup）に備え、キューが空である間は while で待ち直す。
     */
    bool wait_pop_recv(RecvPacket& out_packet, int timeout_ms) {
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

    /**
     * @brief 現在のクライアント数を返す。
     * @return 接続中のクライアント数（切断済みで未回収のものも含む）
     */
    size_t client_count() {
        pthread_mutex_lock(&clients_mutex_);
        size_t n = clients_.size();
        pthread_mutex_unlock(&clients_mutex_);
        return n;
    }

    /**
     * @brief スレッドセーフなログ出力（1 行）。
     * @param msg 出力する文字列（改行は自動で付く）
     * @details
     * 複数スレッドが同時に std::cout へ書くと出力が混ざるため、専用 mutex で直列化する。
     * main 側のログもこの関数を通すと、サーバ内部のログと混ざらない。
     */
    void log_line(const std::string& msg) {
        pthread_mutex_lock(&log_mutex_);
        std::cout << msg << std::endl;
        pthread_mutex_unlock(&log_mutex_);
    }

private:
    /**
     * @brief 内部定数。C++03 では in-class で static const 以外の定数を宣言しにくいため enum で定義する。
     */
    enum {
        POLL_MS        = 200,          ///< select のタイムアウト[ms]。停止フラグ／active の確認周期を兼ねる（停止に最大この時間かかる）
        MAX_LINE       = 64 * 1024,    ///< 1 メッセージの最大長[byte]。区切りが来ないまま超えたら切断（メモリ枯渇攻撃・異常クライアント対策）
        SEND_STALL_MS  = 10000,        ///< 送信が全く進まない状態がこの時間続いたら切断[ms]（相手が受信しなくなった場合の対策）
        STACK_SIZE     = 128 * 1024    ///< 各スレッドのスタックサイズ[byte]。クライアント数 × 2 本分のメモリを抑える
    };

    /**
     * @struct ClientContext
     * @brief 1 クライアントぶんの状態。recv/send の 2 スレッドが共有する。
     *
     * @details
     * 所有権: accept スレッド（register_client）が生成し、reap() が両スレッド join 後に delete する。
     * recv/send スレッドはポインタを借りて使うだけで、解放しない。
     *
     * mutex は「active / recv_done / send_done / send_queue」を保護する。
     * id・fd・peer・server・各 pthread_t は生成後に変更されないため、ロック無しで読んでよい。
     */
    struct ClientContext {
        QnxMultiTcpServer* server;      ///< 所属サーバ（スレッドエントリから本体のメンバ関数を呼ぶため）
        int id;                         ///< クライアント ID
        int fd;                         ///< 接続ソケット。close は reap() のみが行う
        std::string peer;               ///< 接続元 "IP:port"（ログ用）
        pthread_t recv_thread;          ///< 受信スレッド（reap で join）
        pthread_t send_thread;          ///< 送信スレッド（reap で join）
        std::queue<std::string> send_queue; ///< 送信待ちデータ（mutex で保護）
        pthread_mutex_t mutex;          ///< 本構造体の可変メンバを保護するミューテックス
        pthread_cond_t cv;              ///< 送信キューへの追加／停止を send スレッドへ知らせる条件変数
        bool active;                    ///< false になったら両スレッドは終了へ向かう（停止要求・切断・エラーのいずれでも false）
        bool recv_done;                 ///< recv スレッドが終了処理まで済ませた
        bool send_done;                 ///< send スレッドが終了処理まで済ませた（両方 true で回収可能）

        /**
         * @brief コンストラクタ。同期オブジェクトを初期化する。
         * @param s    所属サーバ
         * @param cid  クライアント ID
         * @param sock 接続ソケット fd
         */
        ClientContext(QnxMultiTcpServer* s, int cid, int sock)
            : server(s), id(cid), fd(sock),
              active(true), recv_done(false), send_done(false) {
            pthread_mutex_init(&mutex, NULL);
            pthread_cond_init(&cv, NULL);
        }

        /**
         * @brief デストラクタ。同期オブジェクトを破棄する。
         * @note fd の close はここでは行わない（reap() が join 後に明示的に行う）。
         *       待機中スレッドがいる状態で呼ばないこと（reap が join 後に delete するので満たされる）。
         */
        ~ClientContext() {
            pthread_cond_destroy(&cv);
            pthread_mutex_destroy(&mutex);
        }
    private:
        /// コピー禁止（mutex を含むため）。C++03 流に宣言のみで実装しない。
        ClientContext(const ClientContext&);
        ClientContext& operator=(const ClientContext&);
    };

    /// クライアント ID → ClientContext* の対応表。clients_mutex_ で保護する。
    typedef std::map<int, ClientContext*> ClientMap;

    /// サーバ本体のコピー禁止（スレッドが this を保持するため）
    QnxMultiTcpServer(const QnxMultiTcpServer&);
    QnxMultiTcpServer& operator=(const QnxMultiTcpServer&);

    // ------------------------------------------------------------------
    // メンバ変数
    // ------------------------------------------------------------------
    int port_;                      ///< 待ち受けポート
    size_t max_clients_;            ///< 同時接続数上限
    size_t max_recv_queue_;         ///< 受信キュー上限
    size_t max_send_queue_;         ///< クライアントごとの送信キュー上限

    int listen_fd_;                 ///< 待ち受けソケット（start/stop を呼ぶスレッドと accept スレッドが参照。start 時に確定し、accept スレッド停止後にのみ close する）
    bool started_;                  ///< 開始済みか。start()/stop() を呼ぶスレッドのみが触る（排他不要）
    bool stopping_;                 ///< 停止要求フラグ。state_mutex_ で保護（volatile では同期にならないため mutex を使う）
    pthread_mutex_t state_mutex_;   ///< stopping_ の保護用
    pthread_t accept_tid_;          ///< accept スレッド
    int next_client_id_;            ///< 次に割り当てるクライアント ID。accept スレッドのみが触る

    ClientMap clients_;             ///< 管理中のクライアント一覧
    pthread_mutex_t clients_mutex_; ///< clients_ の保護用（ロック順序の最上位）

    std::queue<RecvPacket> recv_queue_; ///< 全クライアント共通の受信キュー
    pthread_mutex_t recv_mutex_;    ///< recv_queue_ の保護用
    pthread_cond_t recv_cv_;        ///< 受信データ到着を wait_pop_recv() へ知らせる条件変数

    pthread_mutex_t log_mutex_;     ///< ログ出力の直列化用

    // ------------------------------------------------------------------
    // ユーティリティ
    // ------------------------------------------------------------------

    /**
     * @brief 整数を文字列にする。
     * @details std::to_string は C++11 なので、C++03 互換のため ostringstream で代用。
     */
    static std::string to_str(long v) {
        std::ostringstream oss;
        oss << v;
        return oss.str();
    }

    /**
     * @brief fd を非ブロッキングにする。
     * @details
     * select() で「準備完了」を確認してから recv/send/accept するが、
     * 稀に準備完了後に状態が変わってブロックしうる。非ブロッキングにして、
     * スレッドが停止要求に反応できなくなる事態を避ける。
     * @retval true 成功
     */
    static bool set_nonblocking(int fd) {
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
     * 停止要求（stopping_ / active）をタイムアウト周期ごとに確認できるようにするための待機手段。
     * 無期限ブロックの recv()/accept() だと、別スレッドからの close/shutdown で確実に解除される保証が
     * QNX 6 では無いため、短いタイムアウト付き select でポーリングする設計にしている。
     */
    static int wait_fd(int fd, bool for_write, int timeout_ms) {
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
    static bool is_retry_errno(int e) {
        return e == EINTR || e == EAGAIN || e == EWOULDBLOCK;
    }

    /// @brief 待ち受けソケットを閉じる（未オープンなら何もしない）。
    void close_listen() {
        if (listen_fd_ != -1) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    /// @brief 停止要求が出ているか（スレッドセーフ）。
    bool is_stopping() {
        pthread_mutex_lock(&state_mutex_);
        bool s = stopping_;
        pthread_mutex_unlock(&state_mutex_);
        return s;
    }

    /// @brief 停止要求フラグを設定する（スレッドセーフ）。
    void set_stopping(bool v) {
        pthread_mutex_lock(&state_mutex_);
        stopping_ = v;
        pthread_mutex_unlock(&state_mutex_);
    }

    /// @brief クライアントがまだ有効（active）か（スレッドセーフ）。
    bool client_active(ClientContext* c) {
        pthread_mutex_lock(&c->mutex);
        bool a = c->active;
        pthread_mutex_unlock(&c->mutex);
        return a;
    }

    /**
     * @brief クライアントの送信キューへデータを積み、send スレッドを起こす。
     * @retval true  積めた
     * @retval false 非 active、またはキュー上限に達している
     * @warning clients_mutex_ を保持した状態で呼ぶこと。
     *          （保持中は reap() が当該クライアントを erase / delete できないため、c が確実に有効）
     */
    bool enqueue_send(ClientContext* c, const std::string& data) {
        bool ok = false;
        pthread_mutex_lock(&c->mutex);
        if (c->active && c->send_queue.size() < max_send_queue_) {
            c->send_queue.push(data);
            pthread_cond_signal(&c->cv);
            ok = true;
        }
        pthread_mutex_unlock(&c->mutex);
        return ok;
    }

    /**
     * @brief 受信キューの先頭を取り出す（ロック済み前提の内部関数）。
     * @retval true 取り出せた / false 空
     * @warning recv_mutex_ を保持した状態で呼ぶこと。
     */
    bool pop_recv_locked(RecvPacket& out) {
        if (recv_queue_.empty()) return false;
        out = recv_queue_.front();
        recv_queue_.pop();
        return true;
    }

    // ------------------------------------------------------------------
    // スレッドエントリ（pthread_create には static 関数しか渡せないため、
    // this / ClientContext* を void* で受けてメンバ関数へ橋渡しする）
    // ------------------------------------------------------------------

    /// @brief accept スレッドのエントリ。arg は QnxMultiTcpServer*。
    static void* accept_entry(void* arg) {
        static_cast<QnxMultiTcpServer*>(arg)->accept_loop();
        return NULL;
    }

    /**
     * @brief recv スレッドのエントリ。arg は ClientContext*。
     * @note 元実装のように new した引数構造体を渡して delete する方式は、
     *       生成失敗時のリークや二重管理の元になるため、ClientContext* を直接渡す。
     */
    static void* recv_entry(void* arg) {
        ClientContext* c = static_cast<ClientContext*>(arg);
        c->server->recv_loop(c);
        return NULL;
    }

    /// @brief send スレッドのエントリ。arg は ClientContext*。
    static void* send_entry(void* arg) {
        ClientContext* c = static_cast<ClientContext*>(arg);
        c->server->send_loop(c);
        return NULL;
    }

    // ------------------------------------------------------------------
    // accept
    // ------------------------------------------------------------------

    /**
     * @brief accept スレッド本体。接続を受け付け、切断済みクライアントを定期的に回収する。
     *
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
    void accept_loop() {
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
     * @brief 新規接続をクライアントとして登録し、recv/send スレッドを起動する。
     *
     * @param fd   accept で得た接続ソケット。呼び出し後の fd の所有権は本関数が持つ
     *             （失敗時は本関数が close するので、呼び出し側は close しないこと）。
     * @param addr 接続元アドレス（ログ用）
     * @retval true  登録成功
     * @retval false 登録失敗（fd は close 済み）
     *
     * @details
     * 失敗経路ごとに「それまでに確保した資源だけを」確実に戻すのがポイント。
     *  - 上限超過／fd 設定失敗 → fd を close するだけ。
     *  - recv スレッド生成失敗 → fd を close、ClientContext を delete。
     *  - send スレッド生成失敗 → 既に動いている recv スレッドを止めて join してから close / delete。
     *
     * clients_ への登録は「両スレッド起動に成功した後」に行う。
     * 登録前にスレッドが先に終了しても、done フラグが立ったまま登録されるため、
     * 次回の reap() で正しく回収される。
     */
    bool register_client(int fd, const struct sockaddr_in& addr) {
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

        ClientContext* c = new ClientContext(this, next_client_id_++, fd);
        c->peer = std::string(inet_ntoa(addr.sin_addr)) + ":" + to_str(ntohs(addr.sin_port));

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setstacksize(&attr, STACK_SIZE);

        if (pthread_create(&c->recv_thread, &attr, recv_entry, c) != 0) {
            pthread_attr_destroy(&attr);
            log_line("[Server] failed to create recv thread");
            close(fd);
            delete c;
            return false;
        }
        if (pthread_create(&c->send_thread, &attr, send_entry, c) != 0) {
            pthread_attr_destroy(&attr);
            log_line("[Server] failed to create send thread");
            // 起動済みの recv スレッドを止めて回収
            pthread_mutex_lock(&c->mutex);
            c->active = false;
            pthread_cond_broadcast(&c->cv);
            pthread_mutex_unlock(&c->mutex);
            shutdown(fd, SHUT_RDWR);
            pthread_join(c->recv_thread, NULL);
            close(fd);
            delete c;
            return false;
        }
        pthread_attr_destroy(&attr);

        pthread_mutex_lock(&clients_mutex_);
        clients_[c->id] = c;
        pthread_mutex_unlock(&clients_mutex_);

        log_line("[Server] Client connected. ID: " + to_str(c->id) + " (" + c->peer + ")");
        return true;
    }

    // ------------------------------------------------------------------
    // recv
    // ------------------------------------------------------------------

    /**
     * @brief recv スレッド本体。ソケットから受信し、メッセージに切り出して受信キューへ積む。
     *
     * @param c 担当クライアント
     *
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
    void recv_loop(ClientContext* c) {
        std::string buf;
        char tmp[4096];

        while (client_active(c)) {
            int r = wait_fd(c->fd, false, POLL_MS);
            if (r < 0) break;
            if (r == 0) continue;

            ssize_t n = recv(c->fd, tmp, sizeof(tmp), 0);
            if (n == 0) break;                         // 相手が正常切断
            if (n < 0) {
                if (is_retry_errno(errno)) continue;
                break;                                 // エラー
            }

            buf.append(tmp, static_cast<size_t>(n));   // バイナリ(\0)も安全（std::string(tmp) だと \0 で途切れる）
            if (!extract_lines(c, buf)) break;
        }

        // 終了通知のみ。close / delete は reap() が行う。
        pthread_mutex_lock(&c->mutex);
        c->active = false;
        c->recv_done = true;
        pthread_cond_broadcast(&c->cv);   // send スレッドを起こす
        pthread_mutex_unlock(&c->mutex);
        shutdown(c->fd, SHUT_RDWR);
    }

    /**
     * @brief 受信バッファから '\n' 区切りでメッセージを切り出し、受信キューへ積む。
     *
     * @param c        送信元クライアント
     * @param[in,out] buf このクライアントの受信バッファ。切り出した分は先頭から削除される
     * @retval true  処理継続可
     * @retval false 切断すべき（行長超過、または停止要求で push 不能）
     *
     * @details
     *  - "\r\n" 区切りにも対応するため、行末の '\r' を取り除く。
     *  - 空行は無視する（keep-alive 的な改行のみの送信などを許容）。
     *  - 区切りが来ないまま MAX_LINE を超えたら異常とみなして false を返す。
     *    これが無いと、改行を送らない相手にメモリを使い尽くされる。
     */
    bool extract_lines(ClientContext* c, std::string& buf) {
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
            log_line("[Server] Client #" + to_str(c->id) + " exceeded max line length; disconnecting");
            return false;
        }
        return true;
    }

    /**
     * @brief 受信キューへ 1 メッセージ積む。キューが満杯なら空くまで待つ（バックプレッシャ）。
     *
     * @param c    送信元クライアント
     * @param line メッセージ本体
     * @retval true  積めた
     * @retval false 待機中にクライアントが非 active になった（呼び出し側は終了へ向かう）
     *
     * @details
     * 満杯時にデータを捨てると「黙ってメッセージが消える」ため採用せず、待機を選んでいる。
     * 待機中は recv() を呼ばないので、カーネルの受信バッファ → TCP ウィンドウの順に埋まり、
     * 送信側が自然に減速する。待機中も 10ms ごとに client_active() を確認し、
     * 停止要求・切断があれば抜ける（main が止まっていても stop() が固まらない）。
     * ロックは「キュー操作の間だけ」保持し、待機（usleep）中は保持しない。
     */
    bool push_recv(ClientContext* c, const std::string& line) {
        for (;;) {
            pthread_mutex_lock(&recv_mutex_);
            if (recv_queue_.size() < max_recv_queue_) {
                recv_queue_.push(RecvPacket(c->id, line));
                pthread_cond_signal(&recv_cv_);
                pthread_mutex_unlock(&recv_mutex_);
                return true;
            }
            pthread_mutex_unlock(&recv_mutex_);
            if (!client_active(c)) return false;
            usleep(10000);
        }
    }

    // ------------------------------------------------------------------
    // send
    // ------------------------------------------------------------------

    /**
     * @brief send スレッド本体。送信キューからデータを取り出して順に送信する。
     *
     * @param c 担当クライアント
     *
     * @details
     * 待機は条件変数（pthread_cond_wait）で行うため、送信データが無い間は CPU を消費しない。
     * 起こされる要因は「キューへの追加（enqueue_send）」と「active=false（停止・切断）」の 2 つ。
     * 待機条件を while で再評価するのは、擬似的な起床（spurious wakeup）に対応するため。
     *
     * 実際の送信（send_all）は mutex を「解放してから」行う。
     * 送信は相手次第で時間がかかるので、保持したままだと async_send() が詰まってしまう。
     *
     * 終了処理は recv_loop と対称で、active=false・send_done=true にするだけ。
     * close / delete は reap() が行う。最後の shutdown() で recv スレッドの select を起こし、
     * 片方が終わればもう片方も速やかに終わるようにしている。
     */
    void send_loop(ClientContext* c) {
        for (;;) {
            std::string data;

            pthread_mutex_lock(&c->mutex);
            while (c->active && c->send_queue.empty()) {
                pthread_cond_wait(&c->cv, &c->mutex);
            }
            if (!c->active) {
                pthread_mutex_unlock(&c->mutex);
                break;
            }
            data = c->send_queue.front();
            c->send_queue.pop();
            pthread_mutex_unlock(&c->mutex);

            if (!send_all(c, data)) break;
        }

        pthread_mutex_lock(&c->mutex);
        c->active = false;
        c->send_done = true;
        pthread_cond_broadcast(&c->cv);
        pthread_mutex_unlock(&c->mutex);
        shutdown(c->fd, SHUT_RDWR);   // recv スレッドを速やかに終了させる
    }

    /**
     * @brief data を全量送り切る。
     *
     * @param c    宛先クライアント
     * @param data 送信データ
     * @retval true  全量送信できた
     * @retval false 切断・エラー・停止要求・送信停滞（SEND_STALL_MS 超）のいずれか
     *
     * @details
     * send() は要求したバイト数より少なく返すことがある（部分送信）。
     * そのため、送った分だけ off を進めて全量送れるまで繰り返す。
     * EINTR / EAGAIN は再試行する（切断扱いにしない）。
     * 書き込み可能を select() で待ち、タイムアウトのたびに停止要求を確認する。
     * 「全く進まない状態」が SEND_STALL_MS 続いたら、受信しなくなった相手とみなして切断する
     * （相手が詰まったまま送信スレッドが永久に居座るのを防ぐ）。進捗があれば stalled_ms をリセットする。
     */
    bool send_all(ClientContext* c, const std::string& data) {
        size_t off = 0;
        int stalled_ms = 0;
        while (off < data.size()) {
            if (!client_active(c)) return false;

            int r = wait_fd(c->fd, true, POLL_MS);
            if (r < 0) return false;
            if (r == 0) {
                stalled_ms += POLL_MS;
                if (stalled_ms >= SEND_STALL_MS) {
                    log_line("[Server] Client #" + to_str(c->id) + " send stalled; disconnecting");
                    return false;
                }
                continue;
            }

            ssize_t n = send(c->fd, data.data() + off, data.size() - off, QNX_MTS_SEND_FLAGS);
            if (n < 0) {
                if (is_retry_errno(errno)) continue;
                return false;
            }
            off += static_cast<size_t>(n);
            stalled_ms = 0;
        }
        return true;
    }

    // ------------------------------------------------------------------
    // 回収
    // ------------------------------------------------------------------

    /**
     * @brief クライアントの資源を回収する。fd の close と ClientContext の delete を行う「唯一の場所」。
     *
     * @param all false: 両スレッドが終了済み（recv_done && send_done）のクライアントだけ回収する。
     *            true : stop() 用。全クライアントを回収する（呼び出し前に active=false 等で停止通知済みであること）。
     *
     * @details
     * 手順を 2 段階に分けているのがポイント:
     *  -# clients_mutex_ を保持して clients_ から外し、回収リスト dead に移す（短時間のみロック）。
     *  -# ロックを解放してから join / close / delete を行う。
     *     join は相手スレッドの終了待ちで時間がかかりうるため、ロック保持のまま行うと
     *     その間 async_send() など clients_mutex_ を使う処理がすべて止まってしまう。
     *
     * 先に clients_ から外してあるので、以後 async_send() からは見えず、
     * 他スレッドがこのクライアントを参照することは無い（安全に delete できる）。
     *
     * @note std::map の走査中の erase は、C++03 では erase(it++) の形で行う
     *       （erase 後に it を進めると無効なイテレータを触ってしまうため）。
     */
    void reap(bool all) {
        std::vector<ClientContext*> dead;

        pthread_mutex_lock(&clients_mutex_);
        ClientMap::iterator it = clients_.begin();
        while (it != clients_.end()) {
            ClientContext* c = it->second;
            bool done;
            pthread_mutex_lock(&c->mutex);
            done = c->recv_done && c->send_done;
            pthread_mutex_unlock(&c->mutex);

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
            pthread_join(c->recv_thread, NULL);
            pthread_join(c->send_thread, NULL);
            close(c->fd);
            log_line("[Server] Client disconnected. ID: " + to_str(c->id));
            delete c;
        }
    }
};

#endif // QNX_MULTI_TCP_SERVER_H
