package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;

/**
 * {@link HttpPostProducer} 的工厂类（工厂方法模式）。
 *
 * <p>Maxwell 在 {@code --producer=custom} 时不会写死具体 Producer，而是：
 * <ol>
 *   <li>读取 {@code --custom_producer.factory=全限定类名}</li>
 *   <li>反射实例化实现了 {@link ProducerFactory} 的类</li>
 *   <li>调用 {@link #createProducer(MaxwellContext)} 得到真正的 {@link AbstractProducer}</li>
 * </ol>
 * 因此本类的职责只有「创建」，发送逻辑全部在 {@link HttpPostProducer}。
 *
 * <p>启动示例：
 * <pre>
 * --producer=custom
 * --custom_producer.factory=com.zendesk.maxwell.producer.HttpPostProducerFactory
 * --custom_producer.url=http://127.0.0.1:8088/maxwell
 * </pre>
 * 其中 {@code custom_producer.url} 会进入 {@code customProducerProperties}，
 * 由 {@link HttpPostProducer} 构造函数读取；工厂本身不解析该参数。
 */
public class HttpPostProducerFactory implements ProducerFactory {
	/**
	 * 由 Maxwell 核心调用；每次需要自定义生产者时创建 {@link HttpPostProducer} 实例。
	 *
	 * @param context 运行上下文（含配置、位点、指标等），原样交给 Producer 构造函数
	 * @return 可 push 行变更的 HTTP 生产者
	 */
	@Override
	public AbstractProducer createProducer(MaxwellContext context) {
		// 工厂方法：把「如何 new」集中在这里，核心只依赖 ProducerFactory 接口
		return new HttpPostProducer(context);
	}
}