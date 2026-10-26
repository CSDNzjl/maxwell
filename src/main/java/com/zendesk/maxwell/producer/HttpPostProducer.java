package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/**
 * HTTP 自定义生产者：把每条 CDC 变更以表单字段 {@code message}（URL 编码的 JSON）
 * POST 到本地 HTTP 接口（默认 {@code http://127.0.0.1:8088/maxwell}）。
 *
 * <p>设计角色：作为「策略」实现，继承 {@link AbstractProducer}，供 Maxwell 核心在
 * 解析 binlog 后调用 {@link #push(RowMap)}。真正创建本类的是工厂
 * {@link HttpPostProducerFactory}（工厂方法模式），核心代码不直接 {@code new} 本类。
 *
 * <p>与 Spring 对接约定：接收端写成
 * {@code @PostMapping("/maxwell") public void printMessageForMaxwell(String message)}，
 * 本类发送 {@code Content-Type: application/x-www-form-urlencoded}，字段名必须是
 * {@code message}，这样 Spring 才能按参数名绑定到 {@code String message}。
 *
 * <p>位点（position）约定：只有 HTTP 成功（2xx）后才 {@code context.setPosition(r)}；
 * 失败会抛异常且不推进位点，便于断线后从上次成功位置继续，避免丢事件。
 *
 * <p>配置：启动参数 {@code --custom_producer.url=...} 写入
 * {@code customProducerProperties}，键名为 {@code url}；未配置则用 {@link #DEFAULT_URL}。
 */
public class HttpPostProducer extends AbstractProducer {
	static final Logger LOGGER = LoggerFactory.getLogger(HttpPostProducer.class);

	/** 未配置 custom_producer.url 时的默认推送地址（本机 8088 + /maxwell） */
	static final String DEFAULT_URL = "http://127.0.0.1:8088/maxwell";

	/** 实际 POST 目标，构造时从配置解析一次后固定 */
	private final String url;

	/**
	 * @param context Maxwell 运行上下文；从此读取 custom_producer.* 与位点管理能力
	 */
	public HttpPostProducer(MaxwellContext context) {
		super(context);
		// custom_producer.url → Properties 里的键 "url"（前缀由 Maxwell 解析时剥掉）
		String configured = context.getConfig().customProducerProperties.getProperty("url");
		if (configured == null || configured.trim().isEmpty()) {
			this.url = DEFAULT_URL;
		} else {
			this.url = configured.trim();
		}
		LOGGER.info("HttpPostProducer will POST form field 'message' to {}", this.url);
	}

	/**
	 * Maxwell 每解析出一条行变更就回调这里。
	 * <ul>
	 *   <li>过滤掉不需要输出的行：仍推进位点（与官方其它 Producer 一致）</li>
	 *   <li>真正发出去时：仅成功后 setPosition，失败抛错且不推进</li>
	 * </ul>
	 */
	@Override
	public void push(RowMap r) throws Exception {
		// 按 include/exclude 等输出配置：不需要发的事件只记位点，避免卡住复制进度
		if (!r.shouldOutput(outputConfig)) {
			context.setPosition(r);
			return;
		}

		// 序列化为 Maxwell JSON；极端情况下为 null 时同样只推进位点
		String json = r.toJSON(outputConfig);
		if (json == null) {
			context.setPosition(r);
			return;
		}

		try {
			postFormMessage(json);
			LOGGER.info("HttpPostProducer posted OK database={} table={} type={} jsonChars={} -> {}",
					r.getDatabase(), r.getTable(), r.getRowType(), json.length(), url);
			this.succeededMessageCount.inc();
			this.succeededMessageMeter.mark();
			// 关键：HTTP 成功后才推进 binlog 位点，保证「发出去了」才算消费完成
			context.setPosition(r);
		} catch (Exception e) {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();
			LOGGER.error("HttpPostProducer failed to POST to {}: {}", url, e.getMessage());
			// 故意不 setPosition：让上层感知失败，重试/重启后仍从旧位点续传
			throw e;
		}
	}

	/**
	 * 以 application/x-www-form-urlencoded 发送：message=&lt;URLEncoder(json)&gt;。
	 * 字段名固定为 message，对齐 Spring {@code String message} 参数绑定。
	 */
	private void postFormMessage(String json) throws Exception {
		// Spring 表单绑定依赖参数名 "message"；值必须是 URL 编码后的 JSON 文本
		String form = "message=" + URLEncoder.encode(json, "UTF-8");
		byte[] body = form.getBytes(StandardCharsets.UTF_8);

		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		conn.setConnectTimeout(5000);
		conn.setReadTimeout(15000);
		conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
		conn.setRequestProperty("Content-Length", Integer.toString(body.length));

		try (OutputStream os = conn.getOutputStream()) {
			os.write(body);
			os.flush();
		}

		int code = conn.getResponseCode();
		// 读完响应体再 disconnect，避免连接池/半关闭问题；内容本身可忽略
		InputStream err = conn.getErrorStream();
		if (err != null) {
			while (err.read() != -1) { /* drain */ }
			err.close();
		} else {
			InputStream in = conn.getInputStream();
			if (in != null) {
				while (in.read() != -1) { /* drain */ }
				in.close();
			}
		}
		conn.disconnect();

		if (code < 200 || code >= 300) {
			throw new RuntimeException("HTTP POST to " + url + " returned status " + code);
		}
	}
}