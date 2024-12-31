from . import views
from django.urls import path

urlpatterns = [
    path('manage-dag/', views.manage_dag, name='manage_dag'),
    path('list-dag-runs/', views.list_dag_runs, name='list_dag_runs'),
    path('fail-running-dag/', views.manage_fail_running_dag, name='manage_fail_running_dag'), 
    path('monitor-dag/', views.monitor_dag, name='monitor_dag'),
    path('',views.home,name='home'),
    ]
