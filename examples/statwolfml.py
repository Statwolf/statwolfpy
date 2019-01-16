import statwolf
import os

host = os.environ.get('SW_HOST', 'https://a.statwolf.endpoint/dashboard')
username = os.environ.get('SW_USERNAME', 'the user')
password = os.environ.get('SW_PASSWORD', 'a real password')

# create a new statwolfml instance
ml = statwolf.create({
    "host": host,
    "username": username,
    "password": password
}, "statwolfml")

trainConfig = {
    "acquire": {
        "source": "http",
        "type": "csv",
        "dataset_name": "titanic",
        "index_col": 0,
        "url": "https://www.dropbox.com/s/2srdprow69wbawf/titanic.csv?dl=1"
    },
    "preprocess": [{
        "function": "in_set",
        "column": "Embarked",
        "validation_set": ["C", "Q", "S"]
    }, {
        "function": "encode_range",
        "columns": ["Fare"],
        "range": "x.Fare<=7.91",
        "value": 0
    }, {
        "function": "encode_range",
        "columns": ["Fare"],
        "range": "(x.Fare>7.91) & (x.Fare<=14.454)",
        "value": 1
    }, {
        "function": "encode_range",
        "columns": ["Fare"],
        "range": "(x.Fare>14.454) & (x.Fare<=31)",
        "value": 2
    }, {
        "function": "encode_range",
        "columns": ["Fare"],
        "range": "x.Fare>31",
        "value": 3
    }, {
        "function": "convert_to_int",
        "columns": ["Fare"]
    }, {
        "function": "fillna",
        "column": "Embarked",
        "value": "S"
    }, {
        "function": "impute_mean",
        "column": "Age"
    }, {
        "function": "to_round",
        "columns": ["Age"]
    }, {
        "function": "encode_range",
        "columns": ["Age"],
        "range": "x.Age<=16",
        "value": 0
    }, {
        "function": "encode_range",
        "columns": ["Age"],
        "range": "(x.Age>16) & (x.Age <=32)",
        "value": 1
    }, {
        "function": "encode_range",
        "columns": ["Age"],
        "range": "(x.Age>32) & (x.Age <= 48)",
        "value": 2
    }, {
        "function": "encode_range",
        "columns": ["Age"],
        "range": "(x.Age >48) & (x.Age<=64)",
        "value": 3
    }, {
        "function": "encode_range",
        "columns": ["Age"],
        "range": "x.Age >64",
        "value": 4
    },{
        "function": "create_column",
        "new_col": "Family",
        "value": "x['Parch']+x['SibSp']"
    }, {
        "function": "encode_range",
        "columns": ["Family"],
        "range": "x.Family>0",
        "value": 1
    }, {
        "function": "create_column",
        "new_col": "age_class",
        "value": "x['Age']*x['Pclass']"
    }, {
        "function": "label_encoder",
        "columns": ["Sex"]
    }, {
        "function": "drop_columns",
        "columns": ["Name", "Ticket", "Embarked", "Cabin", "SibSp", "Parch", "Sex"]
    }],
    "models": [],
    "models_block": [{
        "target_name": "Survived",
        "accuracy_metrics": ["f1_score"],
        "models": [{
            "name": "logistic_regression",
            "label": "Logistic Regression"
        }, {
            "name": "svc",
            "label": "SVC"
        }, {
            "name": "decision_tree_classifier",
            "label": "Decision Tree Classifier"
        }]
    }],
    "save":[]
}

testConfig = {
    "acquire": {
        "source": "http",
        "type": "csv",
        "dataset_name": "titanic",
        "index_col": 0,
        "url": "https://www.dropbox.com/s/2srdprow69wbawf/titanic.csv?dl=1"
    }
}

model = ml.preprocess(trainConfig)
print(model)
result = ml.apply(model, testConfig)
print(result)
