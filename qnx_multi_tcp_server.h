/**
 * @file    qnx_multi_tcp_server.h
 * @brief   QNX 6.x 向けマルチクライアント TCP サーバ（宣言部）
 *
 * @details
 * 複数クライアントと同時に接続し、次の 2 点を実現するサーバクラス QnxMultiTcpServer と、
 * 受信メッセージを表す RecvPacket を宣言する。実装は qnx_multi_tcp_server.cpp。
 *  - main（アプリケーション）側から「クライアント ID を指定して」非同期に送信できる。
 *  - 受信データは「どのクライアントから来たか（client_id）」付きのキューで受け取れる。
 *
 * このヘッダには「利用者が知るべき契約（引数・戻り値・スレッド安全性・メッセージ仕様）」を書き、
 * 「どう実装しているか・なぜそうしているか」は .cpp 側のコメントに書く方針で分けている。
 *
 * @par 動作環境・方針
 *  - C++03 互換（nullptr / auto / std::to_string / 波括弧初期化は使わない）。
 *  - 依存は POSIX threads / BSD sockets / select() のみ。
 *  - 利用側へ余計なシステムヘッダを持ち込まないよう、ソケット関連の include や内部定数は .cpp に閉じ込めている
 *    （このヘッダが取り込むのは <pthread.h> と標準ライブラリのみ）。
 *
 * @par メッセージ仕様
 *  - 受信: '\n' 区切り（"\r\n" も可）を 1 メッセージとして扱う。RecvPacket::data() には区切り文字を含めない。
 *  - 送信: async_send() は渡された文字列を「そのまま」送る。区切りの '\n' は呼び出し側で付けること。
 *
 * @par スレッド安全性
 *  start() / stop() は同一スレッド（通常は main）からのみ呼ぶこと。
 *  それ以外のメソッド（async_send / async_send_all / try_pop_recv / wait_pop_recv /
 *  client_count / log_line）は複数スレッドから呼んでよい。
 *
 * @par ビルド
 *  qnx_multi_tcp_server.cpp を一緒にコンパイルし、pthread をリンクすること。
 *  QNX 6 では環境により -lsocket が必要になる場合がある。
 *
 * @note ソース内のコメントは UTF-8 で記述している。エディタ／IDE の文字コード設定を確認すること。
 */

#ifndef QNX_MULTI_TCP_SERVER_H
#define QNX_MULTI_TCP_SERVER_H

#include <stddef.h>
#include <pthread.h>

#include <iosfwd>
#include <map>
#include <queue>
#include <string>
#include <vector>

/// accept() で得る接続元アドレス。register_client() の引数型として前方宣言のみ行う（<netinet/in.h> を持ち込まないため）。
struct sockaddr_in;

/**
 * @class RecvPacket
 * @brief 受信キューに積まれる 1 メッセージ。「誰から来たか」と「何が来たか」をひとまとめにしたクラス。
 *
 * @details
 * 設計の意図:
 *  - メンバ変数は private にし、読み取り専用のアクセサ経由で参照させる。
 *    受け取った後にクライアント ID やデータを書き換えて、「別のクライアントから来たこと」にしてしまう事故を防ぐ。
 *  - よく使う操作（前方一致、トリム、区切り分割、整数変換、ログ用文字列化）をメソッドとして用意し、
 *    利用側でコマンド解析のコードを毎回書かずに済むようにする。
 *  - 値型として扱える（コピー・代入可能）。受信キューへの格納や try_pop_recv() の出力引数のために
 *    デフォルトコンストラクタも持つ。
 *  - サーバ本体には依存しない。送信は QnxMultiTcpServer::async_send() に client_id() を渡して行う。
 *
 * @par 使用例
 * @code
 *   RecvPacket p;
 *   if (server.wait_pop_recv(p, 100)) {
 *       std::cout << p << std::endl;                  // "Client #1: SET 10"
 *       if (p.starts_with("SET ")) {
 *           std::vector<std::string> tok = p.split(' ', true);
 *           int v;
 *           if (tok.size() == 2 && RecvPacket(0, tok[1]).to_int(v)) { ... }
 *       }
 *       server.async_send(p.client_id(), "OK\n");
 *   }
 * @endcode
 */
class RecvPacket {
public:
    /**
     * @brief 空のパケットを作る。pop 用の受け皿として使う。
     * @note client_id は 0（＝無効値）。サーバが採番する ID は 1 から始まるため区別できる。
     */
    RecvPacket() : client_id_(0) {}

    /**
     * @brief 値を指定してパケットを作る。
     * @param client_id 送信元クライアント ID
     * @param data      メッセージ本体（区切りの '\n' / "\r\n" は含まない）
     */
    RecvPacket(int client_id, const std::string& data)
        : client_id_(client_id), data_(data) {}

    /// @brief 送信元クライアントの ID を返す。QnxMultiTcpServer::async_send() の宛先にそのまま使える。
    int client_id() const { return client_id_; }

    /// @brief メッセージ本体を返す（区切り文字を含まない）。
    const std::string& data() const { return data_; }

    /// @brief メッセージ本体のバイト数を返す。
    size_t size() const { return data_.size(); }

    /// @brief メッセージ本体が空か。
    bool empty() const { return data_.empty(); }

    /// @brief 有効なパケット（サーバが生成したもの）か。既定構築されただけのパケットは false。
    bool is_valid() const { return client_id_ > 0; }

    /// @brief メッセージが s と完全一致するか。
    bool equals(const std::string& s) const { return data_ == s; }

    /**
     * @brief メッセージが prefix で始まるか（コマンド判定用）。
     * @param prefix 前方一致させる文字列
     * @retval true 一致（prefix が空文字なら常に true）
     */
    bool starts_with(const std::string& prefix) const;

    /**
     * @brief 前後の空白（スペース・タブ・CR・LF）を除いた文字列を返す。元のパケットは変更しない。
     * @return トリム後の文字列（全て空白なら空文字）
     */
    std::string trimmed() const;

    /**
     * @brief メッセージを区切り文字で分割する。
     * @param delim      区切り文字（例: ' ' や ','）
     * @param skip_empty true なら空のトークンを結果に含めない
     * @return 分割後のトークン列。区切りが無ければメッセージ全体を 1 個のトークンとして返す。
     *         空メッセージなら、skip_empty=false で空トークン 1 個、true で空の列。
     */
    std::vector<std::string> split(char delim, bool skip_empty = false) const;

    /**
     * @brief メッセージ全体を 10 進の整数として解釈する。
     * @param[out] out 変換結果（失敗時は変更されない）
     * @retval true  変換成功
     * @retval false 空・数字以外を含む・int の範囲外
     * @note 前後の空白は無視する。
     */
    bool to_int(int& out) const;

    /**
     * @brief ログ出力用の文字列を返す。
     * @return "Client #<id>: <data>" の形式
     */
    std::string to_string() const;

private:
    int client_id_;      ///< 送信元クライアント ID（0 は無効値）
    std::string data_;   ///< メッセージ本体（区切り文字を含まない）
};

/**
 * @brief RecvPacket をストリームへ出力する（ログ用）。RecvPacket::to_string() と同じ書式。
 * @param os 出力先
 * @param p  出力するパケット
 * @return os
 */
std::ostream& operator<<(std::ostream& os, const RecvPacket& p);

/**
 * @class QnxMultiTcpServer
 * @brief 複数クライアント対応の非同期 TCP サーバ。
 *
 * @par スレッド構成
 *  - accept スレッド × 1 : 接続受付と、切断済みクライアントの回収を担当。
 *  - recv スレッド × N   : クライアントごとに 1 本。受信とメッセージ切り出しを担当。
 *  - send スレッド × N   : クライアントごとに 1 本。送信キューを取り出して送信を担当。
 *  - main スレッド       : start()/stop()、async_send()、wait_pop_recv() などを呼ぶ側。
 *
 * @par 使用例
 * @code
 *   QnxMultiTcpServer server(8080);
 *   if (!server.start()) { ...失敗処理...; }
 *   while (running) {
 *       RecvPacket p;
 *       if (server.wait_pop_recv(p, 100)) {
 *           server.async_send(p.client_id(), "reply\n");
 *       }
 *   }
 *   server.stop();
 * @endcode
 *
 * @warning start() / stop() は同一スレッド（通常は main）からのみ呼ぶこと。
 */
class QnxMultiTcpServer {
public:
    /**
     * @brief コンストラクタ。ポート番号と各種上限値を設定し、同期オブジェクトを初期化する。
     *
     * @param port            待ち受けポート番号
     * @param max_clients     同時接続数の上限。超過した接続は accept 直後に切断する。
     *                        クライアント 1 つにつき 2 スレッドを消費するため、機器の資源に合わせて決めること。
     * @param max_recv_queue  受信キューの上限（メッセージ数）。満杯の間は recv スレッドが待機し、
     *                        TCP のフロー制御で送信側が減速する（メッセージは捨てない）。
     * @param max_send_queue  クライアントごとの送信キュー上限（メッセージ数）。満杯時は async_send() が false を返す。
     *
     * @note explicit: int からの暗黙変換でサーバオブジェクトができるのを防ぐ。
     */
    explicit QnxMultiTcpServer(int port,
                               size_t max_clients = 32,
                               size_t max_recv_queue = 1024,
                               size_t max_send_queue = 256);

    /**
     * @brief デストラクタ。稼働中なら stop() で全スレッドを止めてから同期オブジェクトを破棄する。
     */
    ~QnxMultiTcpServer();

    /**
     * @brief サーバを開始する（待ち受けソケットを作り、accept スレッドを起動する）。
     * @retval true  開始成功
     * @retval false 開始失敗（既に開始済み、socket/bind/listen/スレッド生成の失敗など）
     * @note socket / bind / listen は start() 内で同期的に行うため、ポート使用中などの失敗は戻り値で分かる。
     * @warning start()/stop() は同一スレッドから呼ぶこと。
     */
    bool start();

    /**
     * @brief サーバを停止する。全スレッドを join し、全クライアントの資源を解放してから戻る。
     * @note 複数回呼んでも安全（未開始・停止済みなら何もしない）。
     */
    void stop();

    /**
     * @brief 指定クライアントへ非同期でデータを送る（実際の送信はそのクライアントの send スレッドが行う）。
     * @param client_id 宛先クライアント ID（RecvPacket::client_id() で得られる値）
     * @param data      送信データ（そのまま送られる。区切りの '\n' は呼び出し側で付ける）
     * @retval true  送信キューに積めた（相手に届くことの保証ではない）
     * @retval false クライアントが存在しない／切断済み／送信キューが上限に達している
     * @note キューに積むだけで即座に戻るので、ネットワークが詰まっていても呼び出しスレッドをブロックしない。
     */
    bool async_send(int client_id, const std::string& data);

    /**
     * @brief 接続中の全クライアントへ一斉送信する。
     * @param data 送信データ（そのまま送られる）
     * @return 送信キューに積めたクライアント数
     */
    size_t async_send_all(const std::string& data);

    /**
     * @brief 受信キューから 1 件取り出す（ブロックしない）。
     * @param[out] out_packet 取り出したパケットの格納先
     * @retval true  取り出せた
     * @retval false キューが空
     */
    bool try_pop_recv(RecvPacket& out_packet);

    /**
     * @brief 受信キューから 1 件取り出す。空なら最大 timeout_ms ミリ秒待つ。
     * @param[out] out_packet 取り出したパケットの格納先
     * @param      timeout_ms 最大待ち時間[ms]。0 以下なら待たずに try_pop_recv() と同じ動作。
     * @retval true  取り出せた
     * @retval false タイムアウト（または stop() による起床で空のまま）
     */
    bool wait_pop_recv(RecvPacket& out_packet, int timeout_ms);

    /**
     * @brief 現在のクライアント数を返す。
     * @return 接続中のクライアント数（切断済みで未回収のものも含む）
     */
    size_t client_count();

    /**
     * @brief スレッドセーフなログ出力（1 行）。
     * @param msg 出力する文字列（改行は自動で付く）
     * @note main 側のログもこの関数を通すと、サーバ内部のログと出力が混ざらない。
     */
    void log_line(const std::string& msg);

private:
    class ClientContext;    ///< 1 クライアントぶんの状態を持つクラス（定義は .cpp。利用側に内部構造を見せない）

    /// クライアント ID → ClientContext* の対応表。clients_mutex_ で保護する。
    typedef std::map<int, ClientContext*> ClientMap;

    /// コピー禁止（スレッドが this を保持するため）。C++03 流に宣言のみで実装しない。
    QnxMultiTcpServer(const QnxMultiTcpServer&);
    QnxMultiTcpServer& operator=(const QnxMultiTcpServer&);

    // ---- 内部ユーティリティ ----
    void close_listen();                                  ///< 待ち受けソケットを閉じる（未オープンなら何もしない）
    bool is_stopping();                                   ///< 停止要求が出ているか（スレッドセーフ）
    void set_stopping(bool v);                            ///< 停止要求フラグを設定（スレッドセーフ）
    bool pop_recv_locked(RecvPacket& out);                ///< 受信キュー先頭を取り出す。@warning recv_mutex_ 保持中に呼ぶこと

    // ---- スレッドエントリ（pthread_create には static 関数しか渡せないため、void* からメンバ関数へ橋渡しする）----
    static void* accept_entry(void* arg);                 ///< accept スレッドのエントリ（arg は QnxMultiTcpServer*）
    static void* recv_entry(void* arg);                   ///< recv スレッドのエントリ（arg は ClientContext*）
    static void* send_entry(void* arg);                   ///< send スレッドのエントリ（arg は ClientContext*）

    // ---- スレッド本体・処理 ----
    void accept_loop();                                   ///< 接続受付ループ＋切断済みクライアントの回収
    bool register_client(int fd, const struct sockaddr_in& addr); ///< 新規接続の登録とスレッド起動（fd の所有権を引き取る）
    void recv_loop(ClientContext* c);                     ///< 受信ループ
    bool extract_lines(ClientContext* c, std::string& buf); ///< 受信バッファから '\n' 区切りでメッセージを切り出す
    bool push_recv(ClientContext* c, const std::string& line); ///< 受信キューへ積む（満杯なら待機）
    void send_loop(ClientContext* c);                     ///< 送信ループ
    bool send_all(ClientContext* c, const std::string& data);  ///< 全量送信（部分送信・EINTR 対応）
    void reap(bool all);                                  ///< 終了したクライアントの回収（fd の close と delete を行う唯一の場所）

    // ---- メンバ変数 ----
    int port_;                      ///< 待ち受けポート
    size_t max_clients_;            ///< 同時接続数上限
    size_t max_recv_queue_;         ///< 受信キュー上限
    size_t max_send_queue_;         ///< クライアントごとの送信キュー上限

    int listen_fd_;                 ///< 待ち受けソケット
    bool started_;                  ///< 開始済みか。start()/stop() を呼ぶスレッドのみが触る（排他不要）
    bool stopping_;                 ///< 停止要求フラグ。state_mutex_ で保護
    pthread_mutex_t state_mutex_;   ///< stopping_ の保護用
    pthread_t accept_tid_;          ///< accept スレッド
    int next_client_id_;            ///< 次に割り当てるクライアント ID。accept スレッドのみが触る

    ClientMap clients_;             ///< 管理中のクライアント一覧
    pthread_mutex_t clients_mutex_; ///< clients_ の保護用（ロック順序の最上位）

    std::queue<RecvPacket> recv_queue_; ///< 全クライアント共通の受信キュー
    pthread_mutex_t recv_mutex_;    ///< recv_queue_ の保護用
    pthread_cond_t recv_cv_;        ///< 受信データ到着を wait_pop_recv() へ知らせる条件変数

    pthread_mutex_t log_mutex_;     ///< ログ出力の直列化用
};

#endif // QNX_MULTI_TCP_SERVER_H
