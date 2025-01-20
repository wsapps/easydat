package cn.easydat.etl.process.consumer;

public interface CustomTransform {

	Object[] handle(Object[] data);
}
