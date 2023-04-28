package Util.cardinality;


public interface IBuilder<T>
{
    T build();

    int sizeof();
}
