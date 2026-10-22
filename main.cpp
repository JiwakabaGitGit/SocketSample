/**
 * @file    main.cpp
 * @brief   QnxMultiTcpServer の使用例（エコーサーバ）。C++03 互換。
 *
 * @details
 * 受信したメッセージを、送ってきたクライアントにそのまま返す（エコー）。
 * Ctrl+C（SIGINT）または SIGTERM で安全に終了する。
 */
#include <signal.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include "qnx_multi_tcp_server.h"

/**
 * @brief メインループの継続フラグ。シグナルハンドラから書き換える。
 * @details
 * シグナルハンドラから安全に書けるのは volatile sig_atomic_t のみ（標準で保証された型）。
 * mutex や std::cout などはシグナルハンドラ内で使ってはいけない。
 */
static volatile sig_atomic_t g_running = 1;

/**
 * @brief SIGINT / SIGTERM のハンドラ。フラグを下ろすだけにする。
 * @details
 * 実際の終了処理（server.stop()）はハンドラ内では行わず、メインループが
 * フラグを見て抜けた後に通常のコンテキストで行う。ハンドラ内では
 * 非 async-signal-safe な関数（pthread_join など）を呼べないため。
 */
static void on_signal(int) {
    g_running = 0;
}

/**
 * @brief 受信した 1 メッセージを処理する（ここではログ出力＋エコー返信）。
 *
 * @param server 送信に使うサーバ
 * @param packet 受信パケット（packet.client_id() が送信元）
 *
 * @details
 * 返信は packet.client_id() 宛てに async_send() する。送信はキューに積むだけなので
 * main スレッドはネットワーク状況に関わらずブロックしない。
 * 受信側は '\n' 区切りでメッセージを切り出す仕様なので、返信にも末尾に "\n" を付ける。
 * async_send() が false を返すのは、クライアントが既に切断済み、または送信キューが満杯のとき。
 */
static void handle_packet(QnxMultiTcpServer& server, const RecvPacket& packet) {
    // RecvPacket は operator<< を持つので、そのままログへ流せる（"Client #1: hello" 形式）
    std::ostringstream log;
    log << "[Recv] " << packet;
    server.log_line(log.str());   // サーバ内部ログと出力が混ざらないよう log_line を使う

    // 受信したクライアントに個別返信（'\n' 区切りで送る）
    std::ostringstream reply;
    reply << "Echo to #" << packet.client_id() << ": " << packet.data() << "\n";
    if (!server.async_send(packet.client_id(), reply.str())) {
        server.log_line("[Main] reply dropped (client gone or send queue full)");
    }
}

/**
 * @brief エントリポイント。
 * @return 0: 正常終了 / -1: サーバ開始失敗
 */
int main() {
    // SIGINT / SIGTERM を捕捉してフラグを下ろす。
    // sigaction は signal() よりも挙動が規格で明確（ハンドラが自動で解除されない等）。
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    QnxMultiTcpServer server(8080);
    if (!server.start()) {
        // bind 失敗（ポート使用中）などはここで分かる
        std::cerr << "Failed to start server." << std::endl;
        return -1;
    }

    server.log_line("Multi-Client Server started. (Press Ctrl+C to exit)");

    while (g_running) {
        RecvPacket packet;
        // 最大 100ms ブロック。受信が来れば即起床するので、usleep ポーリングより低遅延・低負荷。
        // 100ms ごとにタイムアウトして g_running を確認するため、Ctrl+C にも素早く反応できる。
        if (!server.wait_pop_recv(packet, 100)) continue;

        // 1 件取れたら、溜まっている分をまとめて処理する（待ち直しによる遅延を避ける）
        do {
            handle_packet(server, packet);
        } while (server.try_pop_recv(packet));
    }

    server.log_line("Shutting down...");
    server.stop();   // 全スレッドを join して戻る。stop() 後は安全に main を抜けられる
    return 0;
}
