from dataclasses import dataclass

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    auc,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.utils import resample

# Preserve historical plotting defaults for model diagnostics.
plt.style.use("dark_background")
plt.rcParams["figure.figsize"] = [16, 9]
plt.rcParams.update({"font.size": 18})
plt.rcParams.update({"text.usetex": True})


@dataclass
class modeling:
    """
    Class for predictive modeling.
    """

    def __init__(self, df, rs=0, timeseries=False):
        """
        Initilization of object, splits into train-test.
        ----------
        paramaters
        ----------
        df: dataframe to model
        rs: random state for reproducability
        timeseries: default True, split data sequentially. If False split by random sample.
        """

        self.df = df
        self.rs = rs
        self.y = self.df.label
        self.X = self.df.drop("label", axis=1)
        if timeseries:
            l = len(self.y)
            cut = int(np.ceil(3 * l / 4))
            self.y_train = self.y.iloc[:cut]
            self.y_test = self.y.iloc[cut:]
            self.X_train = self.X.iloc[:cut]
            self.X_test = self.X.iloc[cut:]
        else:
            self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
                self.X, self.y, random_state=rs
            )
        self.confusion = {}
        self.models = {}
        self.predictions = {}
        self.prediction_scores = {}
        self.scores = {}
        self.precision_recall_thresh = {}
        self.fpr_tpr_thresh = {}

    def resample_train(self, method="up"):
        data = pd.concat([self.X_train, self.y_train], axis=1)
        F = data[data.label == 0]
        T = data[data.label == 1]

        if method == "up":
            upsampled = resample(T, replace=True, n_samples=len(F), random_state=self.rs)
            resampled = pd.concat([F, upsampled])

        if method == "down":
            downsampled = resample(F, replace=True, n_samples=len(T), random_state=self.rs)
            resampled = pd.concat([downsampled, T])

        self.y_train = resampled.label
        self.X_train = resampled.drop("label", axis=1)

    def score(self, model):
        _accuracy = accuracy_score(self.y_test, self.predictions[model])
        _precison = precision_score(self.y_test, self.predictions[model])
        _recall = recall_score(self.y_test, self.predictions[model])
        _f1 = f1_score(self.y_test, self.predictions[model])
        _auc = auc(self.fpr_tpr_thresh[model][0], self.fpr_tpr_thresh[model][1])
        self.scores[model] = {
            "accuracy": _accuracy,
            "precison": _precison,
            "recall": _recall,
            "f1": _f1,
            "auc": _auc,
        }

        return self.scores[model]

    def prt(self, model):
        self.precision_recall_thresh[model] = precision_recall_curve(
            self.y_test, self.prediction_scores[model]
        )

        return self.precision_recall_thresh[model]

    def fptpt(self, model):
        self.fpr_tpr_thresh[model] = roc_curve(self.y_test, self.prediction_scores[model])

        return self.fpr_tpr_thresh[model]

    def score_predictions(self, model):
        estimator = self.models[model]
        if hasattr(estimator, "predict_proba"):
            return estimator.predict_proba(self.X_test)[:, 1]
        if hasattr(estimator, "decision_function"):
            return estimator.decision_function(self.X_test)
        return self.predictions[model]

    def plot_pr_curve(self, model):
        plt.figure()
        plt.plot(
            self.precision_recall_thresh[model][0],
            self.precision_recall_thresh[model][1],
            linewidth=3,
        )
        plt.title("PRC")
        plt.xlabel("Precision")
        plt.ylabel("Recall")

        return plt.show()

    def plot_ro_curve(self, model):
        plt.figure()
        plt.plot(self.fpr_tpr_thresh[model][0], self.fpr_tpr_thresh[model][1], linewidth=3)
        plt.title("ROC")
        plt.xlabel(r"FP \%")
        plt.ylabel(r"TP \%")

        return plt.show()

    def linear_regression(self, mi=1000000, rfe=False):
        self.lr = LogisticRegression(max_iter=mi, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        if rfe:
            self.rfe = RFE(self.lr).fit(self.X_train, self.y_train)
            self.supports = {
                self.X.columns[col]: self.rfe.ranking_[col] for col in range(len(self.X.columns))
            }
        self.models["lr"] = self.lr
        self.predictions["lr"] = self.lr.predict(self.X_test)
        self.prediction_scores["lr"] = self.score_predictions("lr")
        self.confusion["lr"] = confusion_matrix(self.y_test, self.predictions["lr"])
        self.prt("lr")
        self.fptpt("lr")

        return self.lr

    def decision_tree(self, md=100):
        self.dt = DecisionTreeClassifier(max_depth=md, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        self.models["dt"] = self.dt
        self.predictions["dt"] = self.dt.predict(self.X_test)
        self.prediction_scores["dt"] = self.score_predictions("dt")
        self.confusion["dt"] = confusion_matrix(self.y_test, self.predictions["dt"])
        self.prt("dt")
        self.fptpt("dt")

        return self.dt

    def random_forest(self, ne=10):
        self.rf = RandomForestClassifier(n_estimators=ne, random_state=self.rs).fit(
            self.X_train, self.y_train
        )
        self.models["rf"] = self.rf
        self.predictions["rf"] = self.rf.predict(self.X_test)
        self.prediction_scores["rf"] = self.score_predictions("rf")
        self.confusion["rf"] = confusion_matrix(self.y_test, self.predictions["rf"])
        self.prt("rf")
        self.fptpt("rf")

        return self.rf

    def support_vector_classifier(self, c=1, kern="rbf"):
        self.svc = SVC(C=c, kernel=kern, random_state=self.rs).fit(self.X_train, self.y_train)
        self.models["svc"] = self.svc
        self.predictions["svc"] = self.svc.predict(self.X_test)
        self.prediction_scores["svc"] = self.score_predictions("svc")
        self.confusion["svc"] = confusion_matrix(self.y_test, self.predictions["svc"])
        self.prt("svc")
        self.fptpt("svc")

        return self.svc


__all__ = ["modeling"]
