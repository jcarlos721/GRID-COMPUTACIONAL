// Worker Sobel: recebe chunks JSON do Master por TCP, processa NIR na GPU e
// devolve sobel_pixels em SUBMIT_RESULT.

#include <cuda_runtime.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

void sendLine(int fd, const std::string& line, std::mutex& sendMutex) {
    const std::string data = line + "\n";
    std::lock_guard<std::mutex> lock(sendMutex);
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto count = send(fd, data.data() + sent, data.size() - sent, 0);
        if (count <= 0) throw std::runtime_error("Falha ao enviar TCP");
        sent += static_cast<std::size_t>(count);
    }
}

std::string receiveLine(int fd) {
    std::string line;
    char buffer[8192];
    while (true) {
        const auto count = recv(fd, buffer, sizeof(buffer), 0);
        if (count <= 0) throw std::runtime_error("Conexão encerrada pelo Master");
        line.append(buffer, static_cast<std::size_t>(count));
        const auto newline = line.find('\n');
        if (newline != std::string::npos) return line.substr(0, newline);
    }
}

int readInt(const std::string& json, const std::string& key) {
    const auto keyPos = json.find("\"" + key + "\"");
    if (keyPos == std::string::npos) throw std::runtime_error("Campo ausente: " + key);
    const auto colon = json.find(':', keyPos);
    return std::stoi(json.substr(colon + 1));
}

std::string readString(const std::string& json, const std::string& key) {
    const auto keyPos = json.find("\"" + key + "\"");
    if (keyPos == std::string::npos) return "";
    const auto begin = json.find('"', json.find(':', keyPos) + 1);
    const auto end = json.find('"', begin + 1);
    return json.substr(begin + 1, end - begin - 1);
}

bool readBool(const std::string& json, const std::string& key) {
    const auto keyPos = json.find("\"" + key + "\"");
    if (keyPos == std::string::npos) return false;
    const auto valuePos = json.find(':', keyPos) + 1;
    return json.compare(valuePos, 4, "true") == 0;
}

std::vector<float> readPixelNumbers(const std::string& json) {
    const auto keyPos = json.find("\"pixels\"");
    const auto begin = json.find('[', keyPos);
    if (keyPos == std::string::npos || begin == std::string::npos)
        throw std::runtime_error("Campo pixels ausente");

    std::vector<float> values;
    int depth = 0;
    for (std::size_t i = begin; i < json.size(); ++i) {
        if (json[i] == '[') ++depth;
        else if (json[i] == ']' && --depth == 0) break;
        else if ((json[i] >= '0' && json[i] <= '9') || json[i] == '-' || json[i] == 'N') {
            char* end = nullptr;
            const float value = std::strtof(json.c_str() + i, &end);
            if (end != json.c_str() + i) {
                values.push_back(value);
                i = static_cast<std::size_t>(end - json.c_str()) - 1;
            }
        }
    }
    return values;
}

__global__ void sobelNir(const float* nir, float* output, int width,
                         int imageHeight, int inputRowStart, int inputRows,
                         int rowStart, int rowEnd) {
    const int x = blockIdx.x * blockDim.x + threadIdx.x;
    const int localY = blockIdx.y * blockDim.y + threadIdx.y;
    const int rows = rowEnd - rowStart;
    if (x >= width || localY >= rows) return;

    const int pixel = localY * width + x;
    const int globalY = rowStart + localY;
    output[pixel] = 0.0f;
    if (x == 0 || x == width - 1 || globalY == 0 || globalY == imageHeight - 1) return;

    const int y = globalY - inputRowStart;
    if (y <= 0 || y >= inputRows - 1) return;
    const int i = y * width + x;

    const float gx = -nir[i - width - 1] + nir[i - width + 1]
                     -2.0f * nir[i - 1] + 2.0f * nir[i + 1]
                     -nir[i + width - 1] + nir[i + width + 1];
    const float gy = -nir[i - width - 1] - 2.0f * nir[i - width] - nir[i - width + 1]
                     +nir[i + width - 1] + 2.0f * nir[i + width] + nir[i + width + 1];
    output[pixel] = sqrtf(gx * gx + gy * gy);
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    const int port = argc > 2 ? std::atoi(argv[2]) : 5056;
    const std::string workerId = argc > 3 ? argv[3] : "sobel-cuda-worker";
    int fd = -1;

    try {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("Não foi possível criar socket");
        sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(port);
        if (inet_pton(AF_INET, host, &server.sin_addr) != 1 ||
            connect(fd, reinterpret_cast<sockaddr*>(&server), sizeof(server)) < 0)
            throw std::runtime_error("Não foi possível conectar ao Master");

        std::mutex sendMutex;
        sendLine(fd, "{\"type\":\"REGISTER\",\"worker_id\":\"" + workerId + "\"}", sendMutex);
        const std::string registered = receiveLine(fd);
        if (readString(registered, "type") != "REGISTERED")
            throw std::runtime_error("Master não respondeu REGISTERED");
        std::cout << "Worker " << workerId << " registrado\n";

        while (true) {
            sendLine(fd, "{\"type\":\"GET_TASK\",\"worker_id\":\"" + workerId + "\"}", sendMutex);
            const std::string task = receiveLine(fd);
            const std::string type = readString(task, "type");
            if (type == "NO_TASK") {
                if (readBool(task, "done")) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(250));
                continue;
            }
            if (type != "TASK_ASSIGN") throw std::runtime_error("Resposta inesperada do Master");

            const int taskId = readInt(task, "task_id");
            const int attemptId = readInt(task, "attempt_id");
            const int width = readInt(task, "image_width");
            const int imageHeight = readInt(task, "image_height");
            const int rowStart = readInt(task, "row_start");
            const int rowEnd = readInt(task, "row_end");
            const int inputRowStart = readInt(task, "input_row_start");
            const int inputRows = readInt(task, "input_row_end") - inputRowStart;

            const auto values = readPixelNumbers(task);
            if (values.size() != static_cast<std::size_t>(inputRows * width * 3))
                throw std::runtime_error("Quantidade de pixels não corresponde ao chunk");
            std::vector<float> nir(inputRows * width);
            for (int i = 0; i < inputRows * width; ++i) nir[i] = values[i * 3];
            std::vector<float> result((rowEnd - rowStart) * width);

            std::atomic<bool> processing{true};
            std::thread heartbeat([&] {
                while (processing.load()) {
                    std::this_thread::sleep_for(std::chrono::seconds(2));
                    if (!processing.load()) break;
                    sendLine(fd, "{\"type\":\"HEARTBEAT\",\"worker_id\":\"" + workerId +
                                 "\",\"task_id\":" + std::to_string(taskId) +
                                 ",\"attempt_id\":" + std::to_string(attemptId) + "}", sendMutex);
                }
            });

            const auto started = std::chrono::steady_clock::now();
            float *deviceNir = nullptr, *deviceResult = nullptr;
            cudaMalloc(&deviceNir, nir.size() * sizeof(float));
            cudaMalloc(&deviceResult, result.size() * sizeof(float));
            cudaMemcpy(deviceNir, nir.data(), nir.size() * sizeof(float), cudaMemcpyHostToDevice);

            const dim3 threads(38, 8);
            const int outputRows = rowEnd - rowStart;
            const dim3 blocks((width + threads.x - 1) / threads.x,
                              (outputRows + threads.y - 1) / threads.y);
            sobelNir<<<blocks, threads>>>(deviceNir, deviceResult, width, imageHeight,
                                          inputRowStart, inputRows, rowStart, rowEnd);
            cudaDeviceSynchronize();
            cudaMemcpy(result.data(), deviceResult, result.size() * sizeof(float), cudaMemcpyDeviceToHost);
            cudaFree(deviceNir);
            cudaFree(deviceResult);
            processing.store(false);
            heartbeat.join();

            const double elapsedMs = std::chrono::duration<double, std::milli>(
                std::chrono::steady_clock::now() - started).count();
            std::ostringstream submit;
            submit << std::setprecision(8)
                   << "{\"type\":\"SUBMIT_RESULT\",\"worker_id\":\"" << workerId
                   << "\",\"task_id\":" << taskId << ",\"attempt_id\":" << attemptId
                   << ",\"row_start\":" << rowStart << ",\"row_end\":" << rowEnd
                   << ",\"elapsed_ms\":" << elapsedMs << ",\"sobel_pixels\":[";
            for (std::size_t i = 0; i < result.size(); ++i) {
                if (i) submit << ',';
                submit << (std::isfinite(result[i]) ? result[i] : 0.0f);
            }
            submit << "]}";
            sendLine(fd, submit.str(), sendMutex);
            const std::string ack = receiveLine(fd);
            std::cout << "Chunk " << taskId << ": " << readString(ack, "type") << '\n';
        }
        close(fd);
        std::cout << "Todas as tarefas Sobel foram concluídas\n";
    } catch (const std::exception& error) {
        if (fd >= 0) close(fd);
        std::cerr << "Erro: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
