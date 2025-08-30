class NodeNotFoundException(Exception):
    """节点不存在异常"""

    pass


class NodeExistsException(Exception):
    """节点已存在异常"""

    pass


class CycleDetectedException(Exception):
    """检测到循环依赖异常"""

    pass


class DagGraph:
    def __init__(self, nodes: list | None = None) -> None:
        """
        初始化DAG图

        Args:
            nodes: 初始节点列表，默认为空列表
        """
        if nodes is None:
            nodes = []
        self.__nodes = set(nodes)  # 使用集合提升查找效率
        self.__edges = set()  # 使用集合存储边
        self.__adjacency_list = {}  # 邻接表，用于快速遍历
        for node in nodes:
            self.__adjacency_list[node] = set()

    def add_node(self, node: str) -> None:
        """
        添加节点

        Args:
            node: 节点名称

        Raises:
            NodeExistsException: 节点已存在
        """
        if node in self.__nodes:
            raise NodeExistsException(f"节点已存在:{node}")
        self.__nodes.add(node)
        if node not in self.__adjacency_list:
            self.__adjacency_list[node] = set()

    def remove_node(self, node: str) -> None:
        """
        删除节点及其相关边

        Args:
            node: 节点名称

        Raises:
            NodeNotFoundException: 节点不存在
        """
        if node not in self.__nodes:
            raise NodeNotFoundException(f"节点不存在:{node}")

        # 删除节点
        self.__nodes.discard(node)

        # 删除与该节点相关的所有边
        edges_to_remove = {(f, t) for f, t in self.__edges if f == node or t == node}
        self.__edges -= edges_to_remove

        # 更新邻接表
        if node in self.__adjacency_list:
            del self.__adjacency_list[node]
        for adjacent in self.__adjacency_list:
            self.__adjacency_list[adjacent].discard(node)

    def add_edge(self, _from: str, _to: str) -> None:
        """
        添加边，如果节点不存在则自动添加

        Args:
            _from: 起始节点
            _to: 目标节点

        Raises:
            CycleDetectedException: 如果添加该边会形成环
        """
        # 自动添加不存在的节点
        if _from not in self.__nodes:
            self.add_node(_from)
        if _to not in self.__nodes:
            self.add_node(_to)

        # 检查是否会形成环
        # if self.__would_create_cycle(_from, _to):
        #     raise CycleDetectedException(f"添加边 {_from} -> {_to} 会形成环")

        # 添加边
        edge = (_from, _to)
        self.__edges.add(edge)
        self.__adjacency_list[_from].add(_to)

    def remove_edge(self, _from: str, _to: str) -> None:
        """
        删除边

        Args:
            _from: 起始节点
            _to: 目标节点

        Raises:
            NodeNotFoundException: 节点不存在
        """
        if _from not in self.__nodes:
            raise NodeNotFoundException(f"节点不存在:{_from}")
        if _to not in self.__nodes:
            raise NodeNotFoundException(f"节点不存在:{_to}")

        edge = (_from, _to)
        self.__edges.discard(edge)
        if _from in self.__adjacency_list:
            self.__adjacency_list[_from].discard(_to)

    def get_nodes(self) -> list:
        """
        获取所有节点（按字母排序）

        Returns:
            节点列表
        """
        return sorted(list(self.__nodes))

    def get_edges(self) -> list:
        """
        获取所有边（去重）

        Returns:
            边列表，每个元素为 (from, to) 元组
        """
        return sorted(list(self.__edges))

    def __would_create_cycle(self, from_node: str, to_node: str) -> bool:
        """
        检查添加边是否会形成环

        Args:
            from_node: 起始节点
            to_node: 目标节点

        Returns:
            True 如果会形成环
        """
        if from_node == to_node:
            return True

        # 从 to_node 开始DFS，看能否到达 from_node
        visited = set()
        stack = [to_node]

        while stack:
            current = stack.pop()
            if current == from_node:
                return True
            if current not in visited:
                visited.add(current)
                # 获取当前节点的所有下游节点
                if current in self.__adjacency_list:
                    stack.extend(self.__adjacency_list[current])

        return False

    def has_cycle(self) -> bool:
        """
        检测图中是否存在环

        Returns:
            True 如果存在环
        """
        visited = set()
        rec_stack = set()

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)

            for neighbor in self.__adjacency_list.get(node, set()):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        for node in self.__nodes:
            if node not in visited:
                if dfs(node):
                    return True

        return False

    def _get_mermaid_str(self, edges: set, direction="LR") -> str:
        """
        获取 Mermaid 格式字符串

        Returns:
            Mermaid格式的图描述字符串
        """
        # edges = list(self.__edges)
        mermaid_str = f"graph {direction}\n"
        if not edges:
            return mermaid_str

        for _from, _to in edges:
            mermaid_str += f"    {_from} --> {_to}\n"
        return mermaid_str

    def print_all_edges_to_mermaid(self) -> None:
        """
        输出所有边到Mermaid格式的图描述字符串
        """
        mermaid_str = self._get_mermaid_str(self.__edges)
        print(mermaid_str)

    def print_edges_to_mermaid(self, edges: set) -> None:
        """
        输出指定边到Mermaid格式的图描述字符串
        """
        mermaid_str = self._get_mermaid_str(edges)
        print(mermaid_str)

    def find_related_edges_downstream(self, node: str) -> set:
        """
        后向查找与节点相关的所有边（查找所有下游依赖）

        Args:
            node: 起始节点

        Returns:
            相关边的列表
        """
        if node not in self.__nodes:
            return set()

        from collections import deque

        queue = deque([node])
        visited = set([node])
        all_relations = []

        while queue:
            current = queue.popleft()
            # 找到当前节点作为起点的所有边
            for edge in self.__edges:
                if edge[0] == current:
                    all_relations.append(edge)
                    if edge[1] not in visited:
                        visited.add(edge[1])
                        queue.append(edge[1])

        return set(all_relations)

    def find_related_edges_upstream(self, node: str) -> set:
        """
        前向查找与节点相关的所有边（查找所有上游依赖）

        Args:
            node: 目标节点

        Returns:
            相关边的列表
        """
        if node not in self.__nodes:
            return set()

        from collections import deque

        queue = deque([node])
        visited = set([node])
        all_relations = []

        while queue:
            current = queue.popleft()
            # 找到当前节点作为终点的所有边
            for edge in self.__edges:
                if edge[1] == current:
                    all_relations.append(edge)
                    if edge[0] not in visited:
                        visited.add(edge[0])
                        queue.append(edge[0])

        return set(all_relations)

    def get_mermaidjs_dag(self, title: str = "DAG Visualization") -> str:
        """
        生成包含Mermaid.js可视化的HTML代码

        Args:
            title: HTML页面标题

        Returns:
            包含Mermaid.js可视化的HTML字符串
        """
        # 获取Mermaid图描述
        mermaid_content = self._get_mermaid_str(self.__edges)

        html_content = f"""<!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{title}</title>
        <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{ startOnLoad: true }});
        </script>

    </head>
    <body>
        <div class="mermaid">
        {mermaid_content}
        </div>
    </body>
    </html>"""

        return html_content
