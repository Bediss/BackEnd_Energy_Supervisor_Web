from django.contrib.auth.backends import BaseBackend
from django.conf import settings
from django.contrib.auth.hashers import check_password
from django.contrib.auth.models import User
from back_DB_bridge import Back_DB_bridge
from rest_framework import authentication, exceptions
from django.contrib.auth import authenticate

import datetime as dt
import json
from rest_framework import exceptions
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework_simplejwt.views import TokenObtainPairView
# from projectapp.apis.common import *

class AuthenticationWithoutPassword(BaseBackend):
    
    def authenticate(self, request, username=None):
        if username is None:
            username = request.data.get('username', '')
        try:
            return User.objects.get(username=username)
        except User.DoesNotExist:
            return None

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None

class Custom_Backend(BaseBackend):
    def validate(self, attrs):
        print("****")
        return attrs
    def get_user(self, user_id):
        return user_id

class JWTAuthentication(TokenObtainPairSerializer):
    def validate(self, attrs):
        return super().validate(attrs)
            # return True

    
class MyTokenObtainPairView(TokenObtainPairView):
    serializer_class = JWTAuthentication