# 基于官方 Python 3.10
FROM python:3.10-slim

# 设置工作目录
WORKDIR /app

# 拷贝依赖文件并安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝项目代码
COPY . .

# 时区环境变量
ENV TZ=Asia/Shanghai

# 容器启动命令
CMD ["python3", "bot.py"]
