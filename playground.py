import streamlit as st
import pandas as pd
import plotly.express as px
import seaborn as sns

st.set_page_config(layout="wide")
st.title("Maquiavelli's Playground")
# st.title('_Streamlit_ is :blue[cool] :sunglasses:')

# st.header('This is a header with a divider', divider='rainbow')
# st.header('_Streamlit_ is :blue[cool] :sunglasses:')


#ANR_RATE_OVERVIEW DATASET
df_anr_rate_overview = pd.read_csv(
                            "datasets/anr-rate-overview.csv", 
                            sep="," , 
                            decimal=".")
#REDEFINE COLUMNS
df_anr_rate_overview = df_anr_rate_overview.rename(
                        columns={
                                'date': "date",
                                'anrRate':"anr_rate",
                                'distinctUsers':"anr_rate_distinct_users"
                                })

##FORMAT DATA
df_anr_rate_overview["anr_rate"] = (df_anr_rate_overview["anr_rate"] * 100).apply('{:.2f}'.format).astype(float)
df_anr_rate_overview["date"] = pd.to_datetime(df_anr_rate_overview["date"])
df_anr_rate_overview=df_anr_rate_overview.sort_values("date")

########################################

#CRASH_RATE_OVERVIEW DATASET
df_crash_rate_overview = pd.read_csv(
                            "datasets/crash-rate-overview.csv", 
                            sep="," , 
                            decimal=".")
#REDEFINE COLUMNS
df_crash_rate_overview = df_crash_rate_overview.rename(
                        columns={
                                'date': "date",
                                'crashRate':"crash_rate",
                                'distinctUsers':"crash_rate_distinct_users"
                                })

##FORMAT DATA
df_crash_rate_overview["crash_rate"] = (df_crash_rate_overview["crash_rate"] * 100).apply('{:.2f}'.format).astype(float)
df_crash_rate_overview["date"] = pd.to_datetime(df_crash_rate_overview["date"])
df_crash_rate_overview=df_crash_rate_overview.sort_values("date")


#######################################

#SLOW_START_RATE DATASET
df_slow_start_overview = pd.read_csv(
                                "datasets/slow-start-overview.csv", 
                                sep="," , 
                                decimal=".")
df_slow_start_overview = df_slow_start_overview.pivot(
                                index="date", 
                                columns="startType", 
                                values="slowStartRate")
df_slow_start_overview = df_slow_start_overview.reset_index().rename_axis(index=None, columns=None)

df_slow_start_overview = df_slow_start_overview.rename(
                        columns={
                                'COLD':"slow_start_cold",
                                'HOT':"slow_start_hot",
                                'WARM':"slow_start_warm",
                                })

##FORMAT DATA
df_slow_start_overview["date"] = pd.to_datetime(df_slow_start_overview["date"])
df_slow_start_overview["slow_start_cold"] = (df_slow_start_overview[
                                    "slow_start_cold"]* 100).apply('{:.2f}'.format).astype(float)
df_slow_start_overview["slow_start_hot"] = (df_slow_start_overview[
                                    "slow_start_hot"]* 100).apply('{:.2f}'.format).astype(float)
df_slow_start_overview["slow_start_warm"] = (df_slow_start_overview[
                                    "slow_start_warm"]* 100).apply('{:.2f}'.format).astype(float)


#######################################
#DEFINE FULL DATASET OVERVIEW
df_merge_step1 = pd.merge(
                        df_anr_rate_overview, 
                        df_slow_start_overview,
                        how="left",
                        on="date")
df_merge_step2 = pd.merge(
                        df_merge_step1, 
                        df_crash_rate_overview,
                        how="left",
                        on="date")

df_full_overview= df_merge_step2.sort_values("date")
df_full_overview["month"] = df_full_overview["date"].apply(lambda x: f"{str(x.year)}-{str(x.month)}")
month = st.sidebar.selectbox("Mês", df_full_overview["month"].unique())
df_full_overview_filtered = df_full_overview[df_full_overview["month"] == month]

#######################################
#DASHBOARD
# df_anr_rate_overview_filtered
col1,col2 = st.columns(2)
col3,col4 = st.columns(2)

fig_slow_start = px.line(df_full_overview_filtered, 
                         x="date", 
                         y=["slow_start_cold","slow_start_hot","slow_start_warm", "slow_start_warm"] , 
                         title="Inicialização lenta por dia",
                         markers=False)
col1.plotly_chart(fig_slow_start)

x_vars_anr_rate = ["slow_start_hot","slow_start_warm","slow_start_cold"]
y_vars1_anr_rate = ["anr_rate"]

fig_pairplot_anr_rate = sns.pairplot(data=df_full_overview_filtered, 
                  x_vars=x_vars_anr_rate, 
                  y_vars=y_vars1_anr_rate)

x_vars_crash_rate = ["slow_start_hot","slow_start_warm","slow_start_cold"]
y_vars1_crash_rate = ["crash_rate"]

fig_pairplot_crash_rate = sns.pairplot(data=df_full_overview_filtered, 
                  x_vars=x_vars_crash_rate, 
                  y_vars=y_vars1_crash_rate)

col2.pyplot(fig_pairplot_anr_rate)
col2.pyplot(fig_pairplot_crash_rate)

fig_anrs_rate = px.bar(df_full_overview_filtered, 
                       x="date", 
                       y="anr_rate", 
                       title="Taxa de ANRs por dia")
col3.plotly_chart(fig_anrs_rate)

fig_crashes_rate = px.bar(df_full_overview_filtered, 
                       x="date", 
                       y="crash_rate", 
                       title="Taxa de Crashes por dia")

col4.plotly_chart(fig_crashes_rate)


# fig_pairplot_test = sns.heatmap(df_full_overview_filtered,
#                                 xticklabels=["slow_start_hot","slow_start_warm","slow_start_cold"],
#                                 yticklabels=["crash_rate"])

# col2.pyplot(fig_pairplot_test)


