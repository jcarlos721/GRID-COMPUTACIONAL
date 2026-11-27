// Demonstra REGISTER -> GET_TASK -> HEARTBEAT -> SUBMIT_RESULT via TCP.
// O cliente envia pixels Sobel demonstrativos (zeros); ainda não calcula Sobel.
//
// Planejamento dos chunks para uma imagem de 2035 x 1847 pixels:
// - Cada chunk cobre a largura inteira e 16 linhas centrais de saída.
// - O pacote inclui até uma linha de margem acima e abaixo para o Sobel 3x3,
//   chegando a 18 linhas de entrada por chunk.
// - São ceil(1847 / 16) = 116 tarefas. Com blocos CUDA de 38 x 8 threads,
//   cada chunk cheio usa uma grade de 54 x 2 = 108 blocos.
// - Dividir também a largura em tiles de 32 x 8 criaria cerca de 14.784
//   tarefas e exigiria margem de um pixel nos quatro lados de cada tile.
//   A faixa de largura inteira reduz mensagens e coordenação; 2035 x 16
//   representa o tamanho de um chunk, não um único bloco CUDA.

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

void sendLine(int socketFd, const std::string& line) {
    const std::string message = line + "\n";
    std::size_t sent = 0;
    while (sent < message.size()) {
        const auto count = send(socketFd, message.data() + sent, message.size() - sent, 0);
        if (count <= 0) throw std::runtime_error("Falha ao enviar mensagem TCP");
        sent += static_cast<std::size_t>(count);
    }
}

std::string receiveLine(int socketFd) {
    std::string line;
    char buffer[8192];
    while (true) {
        const auto count = recv(socketFd, buffer, sizeof(buffer), 0);
        if (count <= 0) throw std::runtime_error("Conexão encerrada antes do fim da mensagem");
        line.append(buffer, static_cast<std::size_t>(count));
        const auto newline = line.find('\n');
        if (newline != std::string::npos) return line.substr(0, newline);
    }
}

int readIntField(const std::string& json, const std::string& field) {
    const auto key = json.find("\"" + field + "\"");
    if (key == std::string::npos) throw std::runtime_error("Campo ausente: " + field);
    const auto colon = json.find(':', key);
    return std::stoi(json.substr(colon + 1));
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    const int port = argc > 2 ? std::atoi(argv[2]) : 5056;
    const std::string workerId = "sobel-cpp-demo";

    try {
        const int socketFd = socket(AF_INET, SOCK_STREAM, 0);
        if (socketFd < 0) throw std::runtime_error("Não foi possível criar o socket");

        sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(port);
        if (inet_pton(AF_INET, host, &server.sin_addr) != 1 ||
            connect(socketFd, reinterpret_cast<sockaddr*>(&server), sizeof(server)) < 0) {
            close(socketFd);
            throw std::runtime_error("Não foi possível conectar ao Master");
        }

        sendLine(socketFd, "{\"type\":\"REGISTER\",\"worker_id\":\"" + workerId + "\"}");
        std::cout << "Master: " << receiveLine(socketFd) << '\n';

        sendLine(socketFd, "{\"type\":\"GET_TASK\",\"worker_id\":\"" + workerId + "\"}");
        const std::string task = receiveLine(socketFd);
        std::cout << "Tarefa recebida (" << task.size() << " bytes)\n";
        if (task.find("\"type\":\"TASK_ASSIGN\"") == std::string::npos) {
            std::cout << "Master: " << task << '\n';
            close(socketFd);
            return 0;
        }

        const int taskId = readIntField(task, "task_id");
        const int attemptId = readIntField(task, "attempt_id");
        sendLine(socketFd, "{\"type\":\"HEARTBEAT\",\"worker_id\":\"" + workerId +
                            "\",\"task_id\":" + std::to_string(taskId) +
                            ",\"attempt_id\":" + std::to_string(attemptId) + "}");

        const int rowStart = readIntField(task, "row_start");
        const int rowEnd = readIntField(task, "row_end");
        const int width = readIntField(task, "image_width");
        const int pixelCount = (rowEnd - rowStart) * width;
        std::string demoPixels = "[";
        for (int i = 0; i < pixelCount; ++i) {
            if (i) demoPixels += ',';
            demoPixels += '0';
        }
        demoPixels += ']';
        sendLine(socketFd, "{\"type\":\"SUBMIT_RESULT\",\"worker_id\":\"" + workerId +
                            "\",\"task_id\":" + std::to_string(taskId) +
                            ",\"attempt_id\":" + std::to_string(attemptId) +
                            ",\"row_start\":" + std::to_string(rowStart) +
                            ",\"row_end\":" + std::to_string(rowEnd) +
                            ",\"sobel_pixels\":" + demoPixels + ",\"elapsed_ms\":0.0}");
        std::cout << "Master: " << receiveLine(socketFd) << '\n';
        close(socketFd);
    } catch (const std::exception& error) {
        std::cerr << "Erro: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
