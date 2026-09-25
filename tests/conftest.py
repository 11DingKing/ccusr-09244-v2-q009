import os
import tempfile

# 必须在任何 app 模块导入之前指定测试数据库，app.config 在导入时读取该变量
_TEST_DIR = tempfile.mkdtemp(prefix="robot-data-test-")
os.environ["DATABASE_URL"] = f"sqlite:///{_TEST_DIR}/test.db"
