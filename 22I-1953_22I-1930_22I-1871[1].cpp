#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <sstream>
#include <map>
#include <vector>
#include <iterator>
using namespace std;

pthread_t threads[4];
pthread_mutex_t write_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t read_lock = PTHREAD_MUTEX_INITIALIZER;

// Global map to store the final result
map<string, int> word_count;

void aggregate_reduced_data(map<string, int>& result, map<string, int>& part_result)
{
    for (auto& entry : part_result)
    {
        result[entry.first] += entry.second; // Sum up the occurrences
    }
}

map<string, int> process_reduction(vector<pair<string, vector<int>>>& chunk)
{
    map<string, int> output;
    for (auto& entry : chunk)
    {
        int sum = 0;
        for (int count : entry.second)
        {
            sum += count;
        }
        output[entry.first] = sum;
    }
    return output;
}

void* mapper_thread1(void* input)
{
    string* data = static_cast<string*>(input);
    int pipe_fd = open("PIPE_1", O_WRONLY);
    if (pipe_fd == -1) {
        perror("Failed to open PIPE_1");
        pthread_exit(nullptr);
    }

    pthread_mutex_lock(&write_lock);
    istringstream data_stream(*data);
    string token;
    while (data_stream >> token)
    {
        string entry = token + ",1\n";
        cout << "Mapper 1: " << entry;
        write(pipe_fd, entry.c_str(), entry.size());
    }
    pthread_mutex_unlock(&write_lock);
    close(pipe_fd);
    pthread_exit(nullptr);
}

void* mapper_thread2(void* input)
{
    string* data = static_cast<string*>(input);
    int pipe_fd = open("PIPE_2", O_WRONLY);
    if (pipe_fd == -1)
    {
        perror("Failed to open PIPE_2");
        pthread_exit(nullptr);
    }

    pthread_mutex_lock(&write_lock);
    istringstream data_stream(*data);
    string token;
    while (data_stream >> token)
    {
        string entry = token + ",1\n";
        cout << "Mapper 2: " << entry;
        write(pipe_fd, entry.c_str(), entry.size());
    }
    pthread_mutex_unlock(&write_lock);
    close(pipe_fd);
    pthread_exit(nullptr);
}

void* mapper_thread3(void* input)
{
    string* data = static_cast<string*>(input);
    int pipe_fd = open("PIPE_3", O_WRONLY);
    if (pipe_fd == -1)
    {
        perror("Failed to open PIPE_3");
        pthread_exit(nullptr);
    }

    pthread_mutex_lock(&write_lock);
    istringstream data_stream(*data);
    string token;
    while (data_stream >> token)
    {
        string entry = token + ",1\n";
        cout << "Mapper 3: " << entry;
        write(pipe_fd, entry.c_str(), entry.size());
    }
    pthread_mutex_unlock(&write_lock);
    close(pipe_fd);
    pthread_exit(nullptr);
}

void* reducer_thread(void* arg)
{
    vector<pair<string, vector<int>>>* chunk = static_cast<vector<pair<string, vector<int>>>*>(arg);
    map<string, int> reduced_data = process_reduction(*chunk);

    static int part_count = 1;
    cout << "\nReducer Output (Part " << part_count << "):\n";
    for (auto& entry : reduced_data)
    {
        cout << "(\"" << entry.first << "\", " << entry.second << "), ";
    }
    cout << endl;

    pthread_mutex_lock(&write_lock);
    aggregate_reduced_data(word_count, reduced_data);
    pthread_mutex_unlock(&write_lock);

    part_count++;
    pthread_exit(nullptr);
}

void* shuffler_thread(void* arg)
{
    pthread_mutex_lock(&read_lock);
    int pipe1_fd = open("PIPE_1", O_RDONLY);
    int pipe2_fd = open("PIPE_2", O_RDONLY);
    int pipe3_fd = open("PIPE_3", O_RDONLY);

    if (pipe1_fd == -1 || pipe2_fd == -1 || pipe3_fd == -1)
    {
        perror("Error opening one of the pipes");
        pthread_mutex_unlock(&read_lock);
        pthread_exit(nullptr);
    }

    usleep(10000);

    char buffer1[3000] = {0}, buffer2[3000] = {0}, buffer3[3000] = {0};
    read(pipe1_fd, buffer1, sizeof(buffer1) - 1);
    read(pipe2_fd, buffer2, sizeof(buffer2) - 1);
    read(pipe3_fd, buffer3, sizeof(buffer3) - 1);

    close(pipe1_fd);
    close(pipe2_fd);
    close(pipe3_fd);

    map<string, vector<int>>* merged_data = new map<string, vector<int>>();
    auto process_data = [&](const char* buffer)
    {
        istringstream data_stream(buffer);
        string line;
        while (getline(data_stream, line))
        {
            size_t comma_pos = line.find(',');
            if (comma_pos != string::npos)
            {
                string word = line.substr(0, comma_pos);
                (*merged_data)[word].push_back(1);
            }
        }
    };

    process_data(buffer1);
    process_data(buffer2);
    process_data(buffer3);

    cout << "\nAfter Shuffling:\n";
    for (auto& entry : *merged_data)
    {
        const string& word = entry.first;
        const vector<int>& counts = entry.second;
        cout << "(\"" << word << "\", [";
        for (size_t i = 0; i < counts.size(); ++i) {
            cout << counts[i];
            if (i < counts.size() - 1) cout << ", ";
        }
        cout << "]), ";
    }
    cout << endl;

    vector<pair<string, vector<int>>> part1, part2, part3;
    int total_words = merged_data->size();
    int partition_size = total_words / 3;
    int idx = 0;
    for (auto& entry : *merged_data)
    {
        if (idx < partition_size) part1.push_back(entry);
        else if (idx < 2 * partition_size) part2.push_back(entry);
        else part3.push_back(entry);
        idx++;
    }

    pthread_t reducer1, reducer2, reducer3;
    pthread_create(&reducer1, nullptr, reducer_thread, &part1);
    pthread_create(&reducer2, nullptr, reducer_thread, &part2);
    pthread_create(&reducer3, nullptr, reducer_thread, &part3);

    pthread_join(reducer1, nullptr);
    pthread_join(reducer2, nullptr);
    pthread_join(reducer3, nullptr);

    delete merged_data;
    pthread_mutex_unlock(&read_lock);
    pthread_exit(nullptr);
}

void removeSpecialCharacters(string& str) {
    string result;
    for (char c : str) {
        if (isalnum(c) || isspace(c)) {  
            result += c;
        }
    }
    str = result;  
}


int main()
{
    mkfifo("PIPE_1", 0666);
    mkfifo("PIPE_2", 0666);
    mkfifo("PIPE_3", 0666);

    string input_data;
    cout << "Enter a sentence: ";
    getline(cin, input_data);
    removeSpecialCharacters(input_data);

    istringstream input_stream(input_data);
    string word;
    vector<string> words;

    while (input_stream >> word) {
        words.push_back(word);
    }

    int num_words = words.size();
    int chunk_size = num_words / 3;

    string part1_data, part2_data, part3_data;

    for (int i = 0; i < chunk_size; ++i) {
        part1_data += words[i] + " ";
    }

    for (int i = chunk_size; i < 2 * chunk_size; ++i) {
        part2_data += words[i] + " ";
    }

    for (int i = 2 * chunk_size; i < num_words; ++i) {
        part3_data += words[i] + " ";
    }

    cout << "Part 1: " << part1_data << endl;
    cout << "Part 2: " << part2_data << endl;
    cout << "Part 3: " << part3_data << endl;

    pthread_create(&threads[3], nullptr, shuffler_thread, nullptr);
    pthread_create(&threads[0], nullptr, mapper_thread1, (void*)&part1_data);
    pthread_create(&threads[1], nullptr, mapper_thread2, (void*)&part2_data);
    pthread_create(&threads[2], nullptr, mapper_thread3, (void*)&part3_data);

    pthread_join(threads[0], nullptr);
    pthread_join(threads[1], nullptr);
    pthread_join(threads[2], nullptr);

    pthread_mutex_unlock(&read_lock);
    pthread_join(threads[3], nullptr);

    cout << "\nFinal Aggregated Output:\n";
    for (auto& entry : word_count)
    {
        cout << "(\"" << entry.first << "\", " << entry.second << "), ";
    }
    cout << endl;

    unlink("PIPE_1");
    unlink("PIPE_2");
    unlink("PIPE_3");
    pthread_mutex_destroy(&write_lock);
    pthread_mutex_destroy(&read_lock);

    return 0;
}