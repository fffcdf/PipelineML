import pandas as pd
import mlflow
from sklearn.model_selection import train_test_split
from sklearn.metrics import f1_score
from sklearn.ensemble import RandomForestClassifier
import psycopg2
import mlflow.catboost


def load_data():
    conn = psycopg2.connect(
        host='postgres-clean-data',
        port=5432,
        user='airflow',
        password='airflow',
        dbname='clean'
    )
    quary='SELECT * FROM clean'
    df=pd.read_sql(quary,conn)
    df=df[df['target'].map(df['target'].value_counts())>=10] # удаляем очень редкие классы

    return df


def preprocessing(df, val_size):
    target = 'target'
    X = df.drop(target, axis=1)
    y = df[target]
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=val_size)

    return X_train, X_val, y_train, y_val


def train():
    val_size = 0.25
    mlflow.set_tracking_uri('http://mlflow:5000')
    with mlflow.start_run():
        df=load_data()
        X_train, X_val, y_train, y_val=preprocessing(df,val_size)
        model=RandomForestClassifier()
        model.fit(X_train,y_train)
        f1=f1_score(y_val,model.predict(X_val),average='weighted')
        mlflow.log_param('model_type',type(model).__name__)
        mlflow.log_metric('f1_score_weighted', f1)
        mlflow.sklearn.log_model(
            sk_model=model,
            name="sklearn-model",
            input_example=X_train,
            registered_model_name="sklearn-model-reg",
        )