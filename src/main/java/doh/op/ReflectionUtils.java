package doh.op;


import doh.op.kvop.FlatMapOp;
import doh.op.kvop.MapOp;
import doh.op.kvop.ReduceOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;


public class ReflectionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReflectionUtils.class);

    public static boolean isUnknown(Class clazz) {
        return clazz.equals(UNKNOWN_CLASS);
    }

    public static Type[] getTypes(Class clazz) {
        return clazz.getTypeParameters();
    }

    public static Class getFromKeyClass(Class clazz) {
        return getGenericParameterClass(clazz, getOpAncestor(clazz), 0);
    }

    public static Class getFromValueClass(Class clazz) {
        return getGenericParameterClass(clazz, getOpAncestor(clazz), 1);
    }

    public static Class getToKeyClass(Class clazz) {
        return getGenericParameterClass(clazz, getOpAncestor(clazz), 2);
    }

    public static Class getToValueClass(Class clazz) {
        return getGenericParameterClass(clazz, getOpAncestor(clazz), 3);
    }

    public static Class getOpAncestor(Class clazz) {
        if (MapOp.class.isAssignableFrom(clazz)) {
            return MapOp.class;
        }
        if (FlatMapOp.class.isAssignableFrom(clazz)) {
            return FlatMapOp.class;
        } else if (ReduceOp.class.isAssignableFrom(clazz)) {
            return ReduceOp.class;
        }
        throw new IllegalArgumentException();
    }


    public static List<Field> getAllDeclaredFields(Class clazz) {
        List<Field> fields = new ArrayList<Field>();
        addAllDeclaredFieldsRec(fields, clazz);
        return fields;
    }

    public static void addAllDeclaredFieldsRec(List<Field> fields, Class clazz) {
        fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        if (!clazz.getSuperclass().equals(Object.class)) {
            addAllDeclaredFieldsRec(fields, clazz.getSuperclass());
        }
    }


    /**
     *  весь код ниже бездумно скопирован с http://habrahabr.ru/post/66593/
     *  al the code below copied from http://habrahabr.ru/post/66593/ without even reading it
     */

    /**
     * Для некоторого класса определяет каким классом был параметризован один из его предков с generic-параметрами.
     *
     * @param actualClass    анализируемый класс
     * @param genericClass   класс, для которого определяется значение параметра
     * @param parameterIndex номер параметра
     * @return класс, являющийся параметром с индексом parameterIndex в genericClass
     */
    public static Class getGenericParameterClass(final Class actualClass, final Class genericClass, final int parameterIndex) {
        // Прекращаем работу если genericClass не является предком actualClass.
        if (!genericClass.isAssignableFrom(actualClass.getSuperclass())) {
            throw new IllegalArgumentException("Class " + genericClass.getName() + " is not a superclass of "
                    + actualClass.getName() + ".");
        }

        // Нам нужно найти класс, для которого непосредственным родителем будет genericClass.
        // Мы будем подниматься вверх по иерархии, пока не найдем интересующий нас класс.
        // В процессе поднятия мы будем сохранять в genericClasses все классы - они нам понадобятся при спуске вниз.

        // Проейденные классы - используются для спуска вниз.
        Stack<ParameterizedType> genericClasses = new Stack<ParameterizedType>();

        // clazz - текущий рассматриваемый класс
        Class clazz = actualClass;

        while (true) {
            Type genericSuperclass = clazz.getGenericSuperclass();
            boolean isParameterizedType = genericSuperclass instanceof ParameterizedType;
            if (isParameterizedType) {
                // Если предок - параметризованный класс, то запоминаем его - возможно он пригодится при спуске вниз.
                genericClasses.push((ParameterizedType) genericSuperclass);
            } else {
                // В иерархии встретился непараметризованный класс. Все ранее сохраненные параметризованные классы будут бесполезны.
                genericClasses.clear();
            }
            // Проверяем, дошли мы до нужного предка или нет.
            Type rawType = isParameterizedType ? ((ParameterizedType) genericSuperclass).getRawType() : genericSuperclass;
            if (!rawType.equals(genericClass)) {
                // genericClass не является непосредственным родителем для текущего класса.
                // Поднимаемся по иерархии дальше.
                clazz = clazz.getSuperclass();
            } else {
                // Мы поднялись до нужного класса. Останавливаемся.
                break;
            }
        }

        // Нужный класс найден. Теперь мы можем узнать, какими типами он параметризован.
        Type result = genericClasses.pop().getActualTypeArguments()[parameterIndex];

        while (result instanceof TypeVariable && !genericClasses.empty()) {
            // Похоже наш параметр задан где-то ниже по иерархии, спускаемся вниз.

            // Получаем индекс параметра в том классе, в котором он задан.
            int actualArgumentIndex = getParameterTypeDeclarationIndex((TypeVariable) result);
            // Берем соответствующий класс, содержащий метаинформацию о нашем параметре.
            ParameterizedType type = genericClasses.pop();
            // Получаем информацию о значении параметра.
            result = type.getActualTypeArguments()[actualArgumentIndex];
        }

        if (result instanceof TypeVariable) {
            // Мы спустились до самого низа, но даже там нужный параметр не имеет явного задания.
            // Следовательно из-за "Type erasure" узнать класс для параметра невозможно.
            LOGGER.warn("Unable to resolve type variable " + result + "."
                    + " Try to replace instances of parametrized class with its non-parameterized subtype.");
            return UNKNOWN_CLASS;
        }

        if (result instanceof ParameterizedType) {
            // Сам параметр оказался параметризованным.
            // Отбросим информацию о его параметрах, она нам не нужна.
            result = ((ParameterizedType) result).getRawType();
        }

        if (result == null) {
            // Should never happen. :)
            throw new IllegalStateException("Unable to determine actual parameter type for "
                    + actualClass.getName() + ".");
        }

        if (!(result instanceof Class)) {
            // Похоже, что параметр - массив или что-то еще, что не является классом.
            throw new IllegalStateException("Actual parameter type for " + actualClass.getName() + " is not a Class.");
        }

        return (Class) result;
    }

    public static final Class UNKNOWN_CLASS = UnknownClass.class;

    public static final class UnknownClass {
    }

    ;


    public static int getParameterTypeDeclarationIndex(final TypeVariable typeVariable) {
        GenericDeclaration genericDeclaration = typeVariable.getGenericDeclaration();

        // Ищем наш параметр среди всех параметров того класса, где определен нужный нам параметр.
        TypeVariable[] typeVariables = genericDeclaration.getTypeParameters();
        Integer actualArgumentIndex = null;
        for (int i = 0; i < typeVariables.length; i++) {
            if (typeVariables[i].equals(typeVariable)) {
                actualArgumentIndex = i;
                break;
            }
        }
        if (actualArgumentIndex != null) {
            return actualArgumentIndex;
        } else {
            throw new IllegalStateException("Argument " + typeVariable.toString() + " is not found in "
                    + genericDeclaration.toString() + ".");
        }
    }
}
