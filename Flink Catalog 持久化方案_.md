

# **Flink SQL 元数据持久化深度解析：从临时 Catalog 到持久化自动化配置**

## **引言**

在使用 Apache Flink 与 Apache Fluss 构建实时数据分析平台时，用户经常会遇到一个关于元数据持久性的核心问题。具体表现为，通过 Flink SQL Client (flinksqlcli) 在 Fluss Catalog 中创建的库和表能够持久化，在重启客户端后，只需重新声明 Catalog 即可恢复；然而，在 Flink 默认的 Catalog 中创建的任何对象，都会随着客户端会话的关闭而消失。这一现象揭示了 Flink 与其生态系统中不同组件在元数据管理策略上的根本差异。

本报告旨在为数据工程师和架构师提供一份详尽的技术指南，深入剖析导致上述行为的底层架构原理，并提供一套完整、健壮且可用于生产环境的解决方案。报告将系统性地解决以下两个核心问题：

1. 如何使 Flink SQL Client 中的默认 Catalog 具备与 Fluss Catalog 相同的持久化能力。  
2. 如何实现 Flink SQL Client 会话启动的自动化，免除每次手动创建和注册 Catalog 的重复性工作。

通过本报告，读者将不仅能解决当前面临的操作难题，还能对 Flink 的 Catalog API、Fluss 的元数据管理模型以及生产环境下的最佳实践获得深刻的理解。

## **第一节 Flink Catalog API：元数据管理深度剖析**

要理解元数据持久化的差异，首先必须深入了解 Flink 的 Catalog API。该 API 是 Flink SQL 与外部世界交互的桥梁，其设计理念和不同实现方式直接决定了元数据的生命周期。

### **1.1 Catalog 在 Flink SQL 中的角色：连接外部系统的桥梁**

Apache Flink 的核心定位是一个分布式流处理引擎，而非一个数据存储系统 1。这一架构特点意味着 Flink 自身不负责持久化存储表、视图或函数等元数据。为了让 Flink SQL 能够像传统关系型数据库（RDBMS）一样查询和操作这些对象，就必须引入一个中介层来管理这些元数据信息，这个中介层就是 Catalog API 1。

在现代数据架构中，计算与存储分离已成为主流趋势 1。Flink 的 Catalog 机制正是这一趋势的体现。它提供了一个可插拔的接口，允许 Flink 连接到各种外部系统，并从中读取或向其写入元数据。这些元数据包括表结构（Schema）、分区信息、数据格式以及连接器属性等。当用户执行一条

CREATE TABLE 语句时，正是通过 Catalog 将这张表的定义注册到某个外部元数据存储中。

### **1.2 GenericInMemoryCatalog：理解 Flink 的临时默认设置**

用户遇到的第一个问题——在默认 Catalog 中创建的表随会话关闭而消失——其根源在于 Flink 的默认 Catalog 实现。这并非一个缺陷，而是其设计使然。

Flink SQL Client 启动时，会默认创建一个名为 default\_catalog 的 Catalog，其实例类型为 GenericInMemoryCatalog 1。顾名思义，这个 Catalog 的所有元数据都存储在 Flink SQL Client 进程的

**内存**中 1。这意味着它的生命周期与客户端会话完全绑定。一旦

flinksqlcli 进程终止，其占用的内存被操作系统回收，其中存储的所有库、表定义也随之烟消云散。

这种设计的初衷是为了提供一个轻量级的、无需任何外部依赖的“开箱即用”体验，非常适合快速原型验证、临时查询任务或不需要持久化的教学演示场景 1。然而，对于任何需要跨会话共享或长期保存的元数据，

GenericInMemoryCatalog 显然无法满足要求。

### **1.3 持久化之路：外部 Catalog 概览**

解决默认 Catalog 临时性问题的唯一途径，是使用外部持久化 Catalog。这类 Catalog 将元数据存储在一个独立于 Flink 会话的、持久化的外部系统中，从而确保元数据在客户端重启后依然存在 1。

在 Flink 生态中，最常用且功能最完善的持久化 Catalog 是 **Hive Catalog** 1。它通过连接一个外部的

**Hive Metastore** 服务来持久化元数据。Hive Metastore 通常由一个关系型数据库（如 PostgreSQL 或 MySQL）作为后端存储，使其成为一个高度可靠、可集中管理元数据的中心 1。使用 Hive Catalog 的最大优势在于其广泛的生态兼容性，它不仅能让多个 Flink 会话共享元数据，还能让 Apache Spark、Trino 等其他计算引擎无缝访问同一套表定义，实现真正的互操作性 1。

另一个值得提及的选项是 **JDBC Catalog**。它允许 Flink 直接连接到一个关系型数据库，并将其中的表暴露给 Flink SQL 查询。然而，JDBC Catalog 的一个关键限制是，它主要用于**读取**已存在于数据库中的表的元数据，而**不支持**通过 Flink SQL CREATE TABLE 语句在其中创建新的、持久化的 Flink 表定义 1。这使得它适合用于集成现有业务数据库，但不能作为 Flink 的通用持久化元数据中心。

下表清晰地总结了 Flink 中几种关键 Catalog 实现的特性差异，直观地解释了用户观察到的不同行为。

| 特性 | GenericInMemoryCatalog (默认) | HiveCatalog | FlussCatalog | JdbcCatalog |
| :---- | :---- | :---- | :---- | :---- |
| **持久性** | 无 (临时性) | 是 (持久化) | 是 (持久化) | 不适用 (只读) |
| **元数据存储** | Flink SQL Client 会话内存 | 外部 Hive Metastore (例如，PostgreSQL/MySQL) | 外部 Fluss 集群 (通过 ZooKeeper) | 外部关系型数据库 |
| **主要应用场景** | 临时任务、测试、会话内对象 | Flink、Spark、Trino 的中央持久化元数据中心 | 管理 Fluss 流式存储系统内的表 | 读取关系型数据库中的已有表 |
| **支持 CREATE TABLE** | 是 | 是 | 是 | 否 |
| **跨会话共享** | 否 | 是 | 是 | 是 (仅读取) |

从根本上说，用户遇到的问题源于对 Flink Catalog 模型的误解。在 Flink 的解耦架构中，Catalog 本身只是一个 API 实现或连接器。它的持久性并非自身属性，而是由其连接的**后端系统**决定的。GenericInMemoryCatalog 的后端是易失的客户端内存；HiveCatalog 的后端是持久的 Hive Metastore 数据库；而 FlussCatalog 的后端则是持久化的 Fluss 集群本身。因此，用户的目标不应是“让默认 Catalog 持久化”，而应该是**用一个由持久化系统支持的 Catalog 实现（即 Hive Catalog）来替换默认的内存实现**。

## **第二节 解构 Apache Fluss：架构与持久化模型**

要解决关于 Fluss Catalog 的第二个问题——为何每次都需要手动注册，我们必须深入其内部架构，理解其如何管理元数据以及如何与 Flink 交互。

### **2.1 Fluss：为 Flink 而生的流式存储**

与作为通用消息队列的 Apache Kafka 不同，Apache Fluss 是一个为实时分析场景量身打造的、与 Flink 深度集成的流式存储层 12。它的核心特性，如列式流（Columnar Stream）、实时更新（Real-Time Updates）和流表对偶性（Stream-Table Duality），都是为了解决 Flink 在处理分析型负载时的痛点而设计的 15。

Fluss 的架构主要包含两大核心组件 16：

* **CoordinatorServer**：作为集群的“大脑”，负责元数据管理（如表结构、分区信息）、Tablet 分配、节点管理和权限控制等协调工作。  
* **TabletServer**：作为集群的“肌肉”，直接负责数据的存储、持久化以及为客户端提供读写 I/O 服务。

这种明确的客户端-服务器（Client-Server）架构模式，是理解用户在 Flink SQL Client 中体验的关键。

### **2.2 Fluss Catalog 的持久化机制：ZooKeeper 的核心角色**

Fluss Catalog 中元数据的持久性，源于其 CoordinatorServer 的设计。在当前版本中，Fluss 依赖一个外部的 **Apache ZooKeeper** 集群来完成所有核心元数据的存储和协调任务，这包括集群配置、节点信息、数据库和表的定义等 16。

当用户在 Flink SQL 中执行一条 CREATE TABLE 语句作用于 Fluss Catalog 时，这条命令通过 Flink 客户端发送到 Fluss 的 CoordinatorServer。CoordinatorServer 接收到请求后，会将这张表的元数据写入到 ZooKeeper 中进行持久化。这就是用户观察到“库表都能持久化”的根本原因——元数据实际上并不存储在 Flink 中，而是安全地存放在了独立的、高可用的 ZooKeeper 集群里。Fluss 的服务（Coordinator 和 TabletServer）在启动时，也必须通过 zookeeper.address 参数配置 ZooKeeper 的连接地址，以加入集群并获取元数据信息 17。

### **2.3 CREATE CATALOG 的生命周期：注册一个连接**

这里我们来解答用户最核心的困惑：既然元数据已经持久化在 ZooKeeper 中，为什么每次启动新的 flinksqlcli 会话时，还需要执行 CREATE CATALOG fluss\_catalog... 语句？

关键在于理解这条命令的真正作用。当用户在一个新的 Flink SQL Client 会话中执行该命令时，**它并非在创建或重建远端 Fluss 集群中的持久化元数据**。实际上，这条命令的作用是在**当前 Flink 会话的内存中，注册一个 FlussCatalog 连接器实例**。这个实例包含了连接到外部 Fluss 集群所需的所有信息。其中最重要的参数是 'bootstrap.servers'，它告诉 Flink 客户端应该去哪里寻找 Fluss 的 CoordinatorServer 18。

我们可以用一个简单的类比来理解：这就像在一个数据库客户端工具（如 DBeaver）中配置一个新的数据库连接。您填写的服务器地址、端口、用户名和密码等信息被保存在该工具的本地配置文件中。数据库本身及其中的表是独立且持久存在的。每次您启动这个工具，都需要选择或激活这个连接配置才能与数据库交互。Flink 的 CREATE CATALOG 命令，在会话层面，就扮演了创建这个“连接配置”的角色。由于 Flink SQL Client 默认不会跨会话保存这些临时的连接配置，因此每次重启后，客户端都“忘记”了如何连接到 Fluss 集群，需要用户手动重新“告知”。

这个架构解释了用户观察到的现象：元数据（表定义）因存储在 ZooKeeper 中而持久；而 Catalog 定义（连接信息）因存储在 Flink 客户端的会话内存中而临时。因此，解决方案的核心就变为如何让这个“连接配置”的创建过程自动化。

## **第三节 统一与持久化：配置 Flink SQL 环境的终极方案**

基于前两节的深入分析，我们现在可以提出一个统一的、一劳永逸的解决方案，同时解决用户面临的两个问题。本节将提供详细的、可操作的步骤指南。

### **3.1 核心策略：元数据集中化与启动自动化**

我们的整体策略分为两步，双管齐下：

1. **解决持久化问题**：用一个持久化的 HiveCatalog 替换 Flink 默认的 GenericInMemoryCatalog，使其成为管理通用 Flink 表的、具备持久化能力的默认元数据中心。  
2. **解决自动化问题**：利用 Flink SQL Client 的 sql-client-defaults.yaml 配置文件，在客户端启动时自动注册所有必需的 Catalog（包括新配置的 Hive Catalog 和已有的 Fluss Catalog），并指定持久化的 Hive Catalog 为默认 Catalog。

实施此策略后，用户将获得一个理想的 Flink SQL 环境：任何在默认 Catalog 下创建的表都将自动持久化，同时与 Fluss 集群的连接也无需任何手动命令即可立即可用。

### **3.2 分步指南：实施持久化的 Hive Catalog**

要使用 Hive Catalog，首先需要部署其核心依赖——Hive Metastore 服务。在生产环境中，最稳健的部署方式是将其作为一个独立服务运行，并使用一个可靠的关系型数据库（如 PostgreSQL）作为其后端存储 9。

依赖准备：  
与 Fluss 类似，Hive Catalog 也不是 Flink 的默认捆绑组件。您需要将对应的连接器 JAR 文件（例如 flink-sql-connector-hive-3.1.2\_2.12.jar，版本需与您的 Flink 和 Hive 版本匹配）以及相关的 Hadoop 依赖 JARs 放入 Flink 的 /lib 目录下 8。在基于 Docker 的环境中，这一过程可以被大大简化。  
使用 Docker Compose 部署后端服务：  
为了方便开发和测试，我们提供一个使用 Docker Compose 的完整配置，用于一键启动所有必需的后端服务。这个配置遵循了容器化部署的最佳实践，例如使用命名的 Docker 卷（Volume）来确保数据持久性，以及使用环境变量进行灵活配置 21。  
以下是推荐的 docker-compose.yml 文件，用于部署 PostgreSQL 和 Hive Metastore：

YAML

\# docker-compose.yml for Hive Metastore Backend  
version: "3.8"

services:  
  \# 1\. PostgreSQL Database for Hive Metastore  
  postgres-metastore-db:  
    image: postgres:13  
    container\_name: postgres-metastore-db  
    environment:  
      \- POSTGRES\_DB=metastore\_db  
      \- POSTGRES\_USER=hive  
      \- POSTGRES\_PASSWORD=hive  
    volumes:  
      \- postgres\_metastore\_data:/var/lib/postgresql/data  
    networks:  
      \- flink\_hive\_net

  \# 2\. Hive Metastore Service  
  hive-metastore:  
    image: ververica/hive-metastore:2.3.6  
    container\_name: hive-metastore  
    depends\_on:  
      \- postgres-metastore-db  
    environment:  
      \- DB\_TYPE=postgres  
      \- DB\_URL=jdbc:postgresql://postgres-metastore-db:5432/metastore\_db  
      \- DB\_USER=hive  
      \- DB\_PASS=hive  
      \- HIVE\_METASTORE\_PORT=9083  
    ports:  
      \- "9083:9083"  
    networks:  
      \- flink\_hive\_net

volumes:  
  postgres\_metastore\_data:  
    name: flink\_hive\_metastore\_pg\_data

networks:  
  flink\_hive\_net:  
    driver: bridge

**说明**：

* postgres-metastore-db 服务：运行一个 PostgreSQL 数据库实例。其数据通过挂载一个名为 postgres\_metastore\_data 的 Docker 卷实现持久化，即使容器被删除，数据也不会丢失。  
* hive-metastore 服务：运行 Hive Metastore。它通过环境变量配置连接到上述的 PostgreSQL 数据库，并监听在 9083 端口，等待 Flink Catalog 的连接。  
* networks：定义了一个共享网络 flink\_hive\_net，确保两个服务可以相互通信。

### **3.3 使用 sql-client-defaults.yaml 实现自动化注册**

sql-client-defaults.yaml 文件是 Flink SQL Client 的默认环境配置文件，客户端在每次启动时都会加载它 25。这是我们实现自动化的关键。

该文件的核心配置项包括：

* catalogs：一个 YAML 数组，用于定义一个或多个需要在会话启动时自动注册的 Catalog。每个 Catalog 定义都包含 name、type 以及其他特定于该类型的属性 27。  
* execution：一个配置块，用于设定会话级别的执行参数。其中，current-catalog 属性可以指定会话启动后默认使用的 Catalog 28。

现在，我们将所有部分整合在一起，创建最终的 sql-client-defaults.yaml 文件。请将此文件放置在 Flink 发行版的 /conf 目录下。

**推荐的 sql-client-defaults.yaml 完整配置**：

YAML

\# \==============================================================================  
\# Flink SQL Client Default Environment Configuration  
\# \==============================================================================

\# 定义会话启动时自动注册的 Catalog 列表  
catalogs:  
  \# 1\. 定义一个名为 'hive\_catalog' 的持久化 Hive Catalog  
  \#    它将作为我们新的默认 Catalog  
  \- name: hive\_catalog  
    type: hive  
    \# 指向 Hive Metastore 的 Thrift URI  
    \# 如果您使用了上面的 Docker Compose 配置，地址就是 hive-metastore:9083  
    uri: thrift://hive-metastore:9083  
    \# Flink 需要一个包含 hive-site.xml 的目录，但在连接远程 Metastore 时，  
    \# 很多配置可以通过 'uri' 直接提供，此目录可以为空或包含一个最小化的配置文件。  
    \# 确保 Flink 容器可以访问此路径。  
    hive-conf-dir: /opt/flink/conf/hive-conf   
    default-database: default

  \# 2\. 预定义到 Fluss 集群的连接  
  \#    这样就无需在会话中手动 CREATE CATALOG  
  \- name: fluss\_catalog  
    type: fluss  
    \# 替换为您的 Fluss CoordinatorServer 的地址  
    bootstrap.servers: fluss-coordinator-server:9123  
    default-database: fluss

\# 定义会话的默认执行参数  
execution:  
  planner: blink  
  type: streaming  
  \# 将持久化的 Hive Catalog 设置为当前会话的默认 Catalog  
  current-catalog: hive\_catalog  
  \# 设置默认的数据库  
  current-database: default

**配置解析**：

* 在 catalogs 列表下，我们定义了两个 Catalog：  
  * hive\_catalog：类型为 hive，通过 uri 属性指向我们用 Docker Compose 部署的 Hive Metastore 服务。  
  * fluss\_catalog：类型为 fluss，通过 bootstrap.servers 属性指向您的 Fluss 集群的 CoordinatorServer 地址。  
* 在 execution 配置块下，我们将 current-catalog 设置为 hive\_catalog。

通过这份配置文件，我们实现了从命令式操作到声明式配置的转变。用户不再需要关心会话启动时的初始化步骤，只需定义好期望的环境状态，Flink SQL Client 就会自动完成所有设置。这不仅解决了用户提出的所有问题，还提供了一个更稳健、可重复和适合生产环境的工作流程。

## **第四节 高级考量与最佳实践**

虽然上述方案解决了核心问题，但在将其应用于生产环境时，还需要考虑一些高级主题和最佳实践，以确保系统的稳定性、高可用性和未来的可维护性。

### **4.1 生产级的 ZooKeeper 与 Metastore 部署**

本报告中提供的 Docker Compose 配置非常适合开发和测试，但生产环境对可靠性的要求更高。

* **ZooKeeper 生产实践**：生产环境中的 ZooKeeper 应该部署为一个**集群（Ensemble）**，通常由 3 个或 5 个节点组成，以实现高可用性。为 ZooKeeper 的事务日志（dataLogDir）和数据快照（dataDir）提供专用的低延迟存储（强烈推荐 SSD）至关重要，因为 ZooKeeper 对磁盘写入延迟非常敏感 30。在任何容器化部署（如 Kubernetes）中，都必须使用持久化卷（Persistent Volumes）来存储其数据 31。关键监控指标应包括  
  NumAliveConnections（活跃连接数）、OutstandingRequests（待处理请求数）和 AvgRequestLatency（平均请求延迟），以确保集群健康 30。  
* **Hive Metastore 生产实践**：为其提供支撑的 PostgreSQL 数据库也应配置为高可用模式，例如使用主从复制和自动故障转移机制。对 Metastore 数据库进行定期备份是至关重要的，因为其中包含了所有 Flink 表的元数据，是系统的核心资产。

### **4.2 Fluss 的未来：移除 ZooKeeper 的必然趋势**

尽管 ZooKeeper 功能强大，但它也为分布式系统带来了显著的运维复杂性，这是 Kafka 等系统用户长期以来的一个痛点 33。Fluss 社区清醒地认识到了这一点。

根据 Apache Fluss 的官方路线图，项目有一个明确的长期目标：**移除对 ZooKeeper 的依赖** 16。这一计划旨在通过一个内部自管理的

**Raft 共识协议**来替代 ZooKeeper 的集群协调和领导者选举功能，并利用 Fluss 自身的 KvStore 组件来存储元数据 16。

这一架构演进对用户来说意义重大。它将极大地简化 Fluss 集群的部署和运维，用户不再需要独立管理一个复杂且敏感的 ZooKeeper 集群。这表明 Fluss 社区正朝着更简化、更集成、运维更友好的方向发展，与用户的期望完全一致。虽然当前用户仍需管理 ZooKeeper，但可以预见，未来的版本将带来更佳的运维体验。

## **结论与建议总结**

本报告深入分析了在 Flink SQL Client 中使用默认 Catalog 和 Fluss Catalog 时遇到的元数据持久性差异问题。分析表明，这些现象是 Flink 和 Fluss 各自架构设计选择的直接体现。

* Flink 默认的 GenericInMemoryCatalog 本质上是临时的，其生命周期与客户端会话绑定。  
* Fluss Catalog 的元数据持久性源于其依赖外部 ZooKeeper 集群进行存储。而在 Flink 端执行的 CREATE CATALOG 语句仅为当前会话注册一个临时的连接配置。

为了彻底解决用户面临的持久化和自动化挑战，本报告提出并详细阐述了一套统一的、可用于生产环境的解决方案。核心建议总结如下：

1. **部署持久化元数据后端**：使用 Docker Compose 或其他生产级部署工具，部署一个由 PostgreSQL 数据库支持的独立 Hive Metastore 服务。这将作为 Flink 的中央、持久化元数据存储。  
2. **采用声明式环境配置**：在 Flink 的 /conf 目录下创建或修改 sql-client-defaults.yaml 文件，以实现 Flink SQL Client 的自动化配置。  
   * 在该文件中，使用 catalogs 列表**同时定义**一个指向新部署的 Hive Metastore 的 hive\_catalog 和一个指向现有 Fluss 集群的 fluss\_catalog。  
   * 在 execution 部分，将 current-catalog 设置为 hive\_catalog，从而将持久化的 Hive Catalog 确立为所有新会话的默认工作环境。

通过实施这些建议，用户可以构建一个健壮、高效且易于维护的 Flink SQL 开发与运维环境。该环境不仅解决了元数据丢失和手动重复配置的问题，更与大数据生态系统中的最佳实践保持一致，为未来的扩展和维护奠定了坚实的基础。

#### **Works cited**

1. Catalogs in Flink SQL—A Primer \- Decodable, accessed on July 26, 2025, [https://www.decodable.co/blog/catalogs-in-flink-sql-a-primer](https://www.decodable.co/blog/catalogs-in-flink-sql-a-primer)  
2. Flink SQL Catalogs for Confluent Manager for Apache Flink, accessed on July 26, 2025, [https://docs.confluent.io/platform/current/flink/flink-sql/catalog.html](https://docs.confluent.io/platform/current/flink/flink-sql/catalog.html)  
3. Understand Apache Flink | Confluent Documentation, accessed on July 26, 2025, [https://docs.confluent.io/platform/current/flink/concepts/flink.html](https://docs.confluent.io/platform/current/flink/concepts/flink.html)  
4. A Comprehensive Guide to Apache Flink Table API | by Parin Patel \- Medium, accessed on July 26, 2025, [https://medium.com/@parinpatel094/a-comprehensive-guide-to-apache-flink-table-api-e3f9acf5c866](https://medium.com/@parinpatel094/a-comprehensive-guide-to-apache-flink-table-api-e3f9acf5c866)  
5. Accelerated Integration: Unveiling Flink Connector's API Design and Latest Advances, accessed on July 26, 2025, [https://www.alibabacloud.com/blog/accelerated-integration-unveiling-flink-connectors-api-design-and-latest-advances\_601334](https://www.alibabacloud.com/blog/accelerated-integration-unveiling-flink-connectors-api-design-and-latest-advances_601334)  
6. Apache Flink SQL: A Gentle Introduction | by Giannis Polyzos \- Medium, accessed on July 26, 2025, [https://medium.com/@ipolyzos\_/streaming-sql-with-apache-flink-a-gentle-introduction-8a3af4fa3194](https://medium.com/@ipolyzos_/streaming-sql-with-apache-flink-a-gentle-introduction-8a3af4fa3194)  
7. Hive catalog | Cloudera on Premises, accessed on July 26, 2025, [https://docs.cloudera.com/csa/1.14.0/how-to-flink/topics/csa-hive-catalog.html](https://docs.cloudera.com/csa/1.14.0/how-to-flink/topics/csa-hive-catalog.html)  
8. Catalogs in Flink SQL—Hands On \- Decodable, accessed on July 26, 2025, [https://www.decodable.co/blog/catalogs-in-flink-sql-hands-on](https://www.decodable.co/blog/catalogs-in-flink-sql-hands-on)  
9. Manage Hive catalogs \- Realtime Compute for Apache Flink ..., accessed on July 26, 2025, [https://www.alibabacloud.com/help/en/flink/user-guide/manage-hive-catalogs](https://www.alibabacloud.com/help/en/flink/user-guide/manage-hive-catalogs)  
10. Delta Lake: Home, accessed on July 26, 2025, [https://delta.io/](https://delta.io/)  
11. Catalogs & Databases | Ververica documentation, accessed on July 26, 2025, [https://docs.ververica.com/vvp/user-guide/sql-development/catalogs-databases/](https://docs.ververica.com/vvp/user-guide/sql-development/catalogs-databases/)  
12. Apache Fluss is a streaming storage built for real-time analytics. \- GitHub, accessed on July 26, 2025, [https://github.com/apache/fluss](https://github.com/apache/fluss)  
13. Fluss: Unified Streaming Storage For Next-Generation Data Analytics, accessed on July 26, 2025, [https://www.ververica.com/blog/introducing-fluss](https://www.ververica.com/blog/introducing-fluss)  
14. Apache Fluss™ (Incubating), accessed on July 26, 2025, [https://fluss.apache.org/](https://fluss.apache.org/)  
15. Introducing Fluss: Streaming Storage for Real-Time Analytics \- Alibaba Cloud Community, accessed on July 26, 2025, [https://www.alibabacloud.com/blog/introducing-fluss-streaming-storage-for-real-time-analytics\_601921](https://www.alibabacloud.com/blog/introducing-fluss-streaming-storage-for-real-time-analytics_601921)  
16. Architecture | Apache Fluss™ (Incubating), accessed on July 26, 2025, [https://fluss.apache.org/docs/0.5/concepts/architecture/](https://fluss.apache.org/docs/0.5/concepts/architecture/)  
17. Deploying with Docker | Apache Fluss™ (Incubating), accessed on July 26, 2025, [https://fluss.apache.org/docs/install-deploy/deploying-with-docker/](https://fluss.apache.org/docs/install-deploy/deploying-with-docker/)  
18. Flink DDL | Fluss \- Alibaba Open Source, accessed on July 26, 2025, [https://alibaba.github.io/fluss-docs/docs/engine-flink/ddl/](https://alibaba.github.io/fluss-docs/docs/engine-flink/ddl/)  
19. Deploying Local Cluster | Apache Fluss™ (Incubating), accessed on July 26, 2025, [https://fluss.apache.org/docs/install-deploy/deploying-local-cluster/](https://fluss.apache.org/docs/install-deploy/deploying-local-cluster/)  
20. \[Bug\] Inaccessible externally due to container ip registration when deploying with Docker · Issue \#318 · alibaba/fluss \- GitHub, accessed on July 26, 2025, [https://github.com/alibaba/fluss/issues/319/linked\_closing\_reference?reference\_location=REPO\_ISSUES\_INDEX](https://github.com/alibaba/fluss/issues/319/linked_closing_reference?reference_location=REPO_ISSUES_INDEX)  
21. Persisting container data \- Docker Docs, accessed on July 26, 2025, [https://docs.docker.com/get-started/docker-concepts/running-containers/persisting-container-data/](https://docs.docker.com/get-started/docker-concepts/running-containers/persisting-container-data/)  
22. bitnami/zookeeper \- Docker Image, accessed on July 26, 2025, [https://hub.docker.com/r/bitnami/zookeeper](https://hub.docker.com/r/bitnami/zookeeper)  
23. Mount Docker External Volumes in Confluent Platform, accessed on July 26, 2025, [https://docs.confluent.io/platform/current/installation/docker/operations/external-volumes.html](https://docs.confluent.io/platform/current/installation/docker/operations/external-volumes.html)  
24. Docker Data Persistence: Building a Solid Foundation for Your Applications and Seamless Containerized Workflows \- Roman Glushach, accessed on July 26, 2025, [https://romanglushach.medium.com/docker-data-persistence-building-a-solid-foundation-for-your-applications-and-seamless-6968b3edf854](https://romanglushach.medium.com/docker-data-persistence-building-a-solid-foundation-for-your-applications-and-seamless-6968b3edf854)  
25. BigDataPlatform/anomaly-detection-scripts/Flink/flink-conf/sql-client-defaults.yaml at master, accessed on July 26, 2025, [https://github.com/PeterLipcak/BigDataPlatform/blob/master/anomaly-detection-scripts/Flink/flink-conf/sql-client-defaults.yaml](https://github.com/PeterLipcak/BigDataPlatform/blob/master/anomaly-detection-scripts/Flink/flink-conf/sql-client-defaults.yaml)  
26. flink-sql-gateway/conf/sql-gateway-defaults.yaml at master \- GitHub, accessed on July 26, 2025, [https://github.com/ververica/flink-sql-gateway/blob/master/conf/sql-gateway-defaults.yaml](https://github.com/ververica/flink-sql-gateway/blob/master/conf/sql-gateway-defaults.yaml)  
27. Flink DDL \- Apache Iceberg™, accessed on July 26, 2025, [https://iceberg.apache.org/docs/nightly/flink-ddl/](https://iceberg.apache.org/docs/nightly/flink-ddl/)  
28. flink-sql-gateway/README.md at master \- GitHub, accessed on July 26, 2025, [https://github.com/ververica/flink-sql-gateway/blob/master/README.md](https://github.com/ververica/flink-sql-gateway/blob/master/README.md)  
29. Flink 1.12.0 sql client queries hive table \- Stack Overflow, accessed on July 26, 2025, [https://stackoverflow.com/questions/65387396/flink-1-12-0-sql-client-queries-hive-table](https://stackoverflow.com/questions/65387396/flink-1-12-0-sql-client-queries-hive-table)  
30. Running ZooKeeper in Production \- Confluent Documentation, accessed on July 26, 2025, [https://docs.confluent.io/platform/7.3/zookeeper/deployment.html](https://docs.confluent.io/platform/7.3/zookeeper/deployment.html)  
31. How to persist Zookeeper data in a docker setup via a volume? \- Stack Overflow, accessed on July 26, 2025, [https://stackoverflow.com/questions/52378898/how-to-persist-zookeeper-data-in-a-docker-setup-via-a-volume](https://stackoverflow.com/questions/52378898/how-to-persist-zookeeper-data-in-a-docker-setup-via-a-volume)  
32. Running ZooKeeper, A Distributed System Coordinator \- Kubernetes, accessed on July 26, 2025, [https://kubernetes.io/docs/tutorials/stateful-application/zookeeper/](https://kubernetes.io/docs/tutorials/stateful-application/zookeeper/)  
33. Kafka Docker Explained: Setup, Best Practices & Tips \- DataCamp, accessed on July 26, 2025, [https://www.datacamp.com/tutorial/kafka-docker-explained](https://www.datacamp.com/tutorial/kafka-docker-explained)  
34. Fluss Roadmap, accessed on July 26, 2025, [https://fluss.apache.org/roadmap/](https://fluss.apache.org/roadmap/)