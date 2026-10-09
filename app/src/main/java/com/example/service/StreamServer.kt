package com.example.service

import java.io.DataOutputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

class StreamServer(
    private val port: Int = 8888,
    private val onLog: (tag: String, message: String, isError: Boolean) -> Unit
) {

    private val clients = CopyOnWriteArrayList<Socket>()
    private var serverSocket: ServerSocket? = null
    private var acceptThread: Thread? = null
    private var running = false

    fun start() {
        if (running) return

        running = true
        acceptThread = thread(start = true, name = "MJPEG-Stream-Server") {
            try {
                serverSocket = ServerSocket(port)
                onLog("NET", "Relay server listening on port $port", false)

                while (running) {
                    val client = serverSocket?.accept() ?: break
                    clients.add(client)
                    onLog("NET", "Client connected: ${client.inetAddress.hostAddress}", false)
                }
            } catch (e: Exception) {
                if (running) {
                    onLog("NET", "Stream server error: ${e.message}", true)
                }
            }
        }
    }

    fun sendFrame(frame: ByteArray) {
        if (!running || frame.isEmpty()) return

        val snapshot = clients.toList()
        if (snapshot.isEmpty()) return

        for (client in snapshot) {
            try {
                if (client.isClosed) {
                    removeClient(client)
                    continue
                }

                val output = DataOutputStream(client.getOutputStream())
                output.writeInt(frame.size)
                output.write(frame)
                output.flush()
            } catch (e: Exception) {
                onLog("NET", "Failed to send frame to ${client.inetAddress.hostAddress}: ${e.message}", true)
                removeClient(client)
            }
        }
    }

    fun stop() {
        running = false

        try {
            serverSocket?.close()
        } catch (_: Exception) {}

        for (client in clients) {
            try {
                client.close()
            } catch (_: Exception) {}
        }
        clients.clear()

        onLog("NET", "Relay server stopped", false)
    }

    fun connectedClients(): Int = clients.size

    private fun removeClient(client: Socket) {
        try {
            client.close()
        } catch (_: Exception) {}
        clients.remove(client)
    }
}
